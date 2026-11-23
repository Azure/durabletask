//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------
#nullable enable
namespace DurableTask.AzureStorage.Storage
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Security.Cryptography;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Xml;
    using System.Xml.Linq;
    using Azure;
    using Azure.Core;
    using Azure.Core.Pipeline;
    using Azure.Data.Tables;

    // Azure.Data.Tables does not yet expose Get User Delegation Key or its SAS signer.
    // Wire format: https://learn.microsoft.com/rest/api/storageservices/create-user-delegation-sas
    sealed class TableUserDelegationKey
    {
        internal const string ServiceVersion = "2025-07-05";
        const string SignedKeyObjectIdParameter = "skoid";
        const string SignedKeyTenantIdParameter = "sktid";
        const string SignedKeyStartParameter = "skt";
        const string SignedKeyExpiryParameter = "ske";
        const string SignedKeyServiceParameter = "sks";
        const string SignedKeyVersionParameter = "skv";
        const string TableService = "t";
        readonly Dictionary<string, string> fields;
        readonly byte[] key;

        TableUserDelegationKey(XElement xml)
        {
            string Read(string name) => (string?)xml.Element(name) ?? throw new InvalidOperationException("Missing user delegation key field: " + name);

            this.fields = new Dictionary<string, string>
            {
                [SignedKeyObjectIdParameter] = Read("SignedOid"),
                [SignedKeyTenantIdParameter] = Read("SignedTid"),
                [SignedKeyStartParameter] = Read("SignedStart"),
                [SignedKeyExpiryParameter] = Read("SignedExpiry"),
                [SignedKeyServiceParameter] = Read("SignedService"),
                [SignedKeyVersionParameter] = Read("SignedVersion"),
            };
            if (this.fields[SignedKeyServiceParameter] != TableService)
            {
                throw new InvalidOperationException("Expected a Table user delegation key.");
            }
            this.key = Convert.FromBase64String(Read("Value"));
            this.ExpiresOn = DateTimeOffset.Parse(this.fields[SignedKeyExpiryParameter], CultureInfo.InvariantCulture);
        }

        public DateTimeOffset ExpiresOn { get; }

        // Request a Table delegation key using the provider's Entra credential.
        public static async Task<TableUserDelegationKey> GetAsync(Uri endpoint, TokenCredential credential, TableClientOptions options, DateTimeOffset serverTime, CancellationToken cancellationToken)
        {
            TableAudience audience = options.Audience ?? TableAudience.AzurePublicCloud;
            string resource = audience == TableAudience.AzurePublicCloud ? "https://storage.azure.com"
                : audience == TableAudience.AzureGovernment ? "https://storage.azure.us"
                : audience == TableAudience.AzureChina ? "https://storage.azure.cn"
                : audience.ToString().TrimEnd('/');
            string scope = resource.EndsWith("/.default", StringComparison.OrdinalIgnoreCase)
                ? resource : resource + "/.default";
            // Workload options may contain the SAS fence policy. Key acquisition has its own authenticated
            // pipeline and cannot depend on an existing, unexpired workload SAS.
            var keyOptions = new TableClientOptions { Transport = options.Transport };
            keyOptions.Retry.Mode = options.Retry.Mode;
            keyOptions.Retry.Delay = options.Retry.Delay;
            keyOptions.Retry.MaxDelay = options.Retry.MaxDelay;
            keyOptions.Retry.MaxRetries = options.Retry.MaxRetries;
            keyOptions.Retry.NetworkTimeout = options.Retry.NetworkTimeout;
            HttpPipeline pipeline = HttpPipelineBuilder.Build(keyOptions, new BearerTokenAuthenticationPolicy(credential, scope));
            using HttpMessage message = pipeline.CreateMessage();
            message.Request.Method = RequestMethod.Post;
            message.Request.Uri.Reset(new Uri(endpoint, "?restype=service&comp=userdelegationkey"));
            message.Request.Headers.Add("x-ms-version", ServiceVersion);
            message.Request.Headers.Add("Content-Type", "application/xml");
            var xml = new XElement("KeyInfo",
                new XElement("Start", FormatTime(serverTime.AddMinutes(-15))),
                new XElement("Expiry", FormatTime(serverTime.AddHours(24))));
            message.Request.Content = RequestContent.Create(Encoding.UTF8.GetBytes(xml.ToString(SaveOptions.DisableFormatting)));
            await pipeline.SendAsync(message, cancellationToken).ConfigureAwait(false);
            if (message.Response.Status != 200)
            {
                // Parse Storage XML errors; keep the SDK's parser for other responses.
                RequestFailedDetailsParser? parser = message.Response.Content.ToString().TrimStart().StartsWith("<", StringComparison.Ordinal)
                    ? XmlErrorDetailsParser.Instance : null;
                throw new RequestFailedException(message.Response, innerException: null, detailsParser: parser);
            }
            using var reader = new StreamReader(message.Response.ContentStream!);
            return new TableUserDelegationKey(XElement.Parse(await reader.ReadToEndAsync().ConfigureAwait(false)));
        }

        // Build a signed SAS query string granting read, add, update, and delete access to one table.
        public string Sign(string accountName, string tableName, DateTimeOffset expiresOn)
        {
            string start = this.fields[SignedKeyStartParameter];
            string expiry = FormatTime(expiresOn);
            string permissions = "raud";
            // Empty delegated tenant/user, IP, and partition/row ranges are required positions in the signature.
            string toSign = string.Join(
                "\n",
                permissions,
                start,
                expiry,
                "/table/" + accountName + "/" + tableName.ToLowerInvariant(),
                this.fields[SignedKeyObjectIdParameter],
                this.fields[SignedKeyTenantIdParameter],
                this.fields[SignedKeyStartParameter],
                this.fields[SignedKeyExpiryParameter],
                this.fields[SignedKeyServiceParameter],
                this.fields[SignedKeyVersionParameter],
                "",
                "",
                "",
                "https",
                ServiceVersion,
                "",
                "",
                "",
                "");
            using var hmac = new HMACSHA256(this.key);
            var query = new Dictionary<string, string>(this.fields)
            {
                ["sp"] = permissions,
                ["st"] = start,
                ["se"] = expiry,
                ["spr"] = "https",
                ["sv"] = ServiceVersion,
                ["tn"] = tableName,
                ["sig"] = Convert.ToBase64String(hmac.ComputeHash(Encoding.UTF8.GetBytes(toSign))),
            };
            return string.Join("&", query.Select(pair => pair.Key + "=" + Uri.EscapeDataString(pair.Value)));
        }

        sealed class XmlErrorDetailsParser : RequestFailedDetailsParser
        {
            internal static readonly XmlErrorDetailsParser Instance = new XmlErrorDetailsParser();

            public override bool TryParse(Response response, out ResponseError? error, out IDictionary<string, string>? data)
            {
                error = null;
                data = null;
                try
                {
                    XElement body = XElement.Parse(response.Content.ToString());
                    string? code = (string?)body.Element("Code");
                    if (body.Name != "Error" || string.IsNullOrEmpty(code))
                    {
                        return false;
                    }
                    error = new ResponseError(code, (string?)body.Element("Message") ?? "Service request failed.");
                    return true;
                }
                catch (XmlException)
                {
                    // Keep the original HTTP failure when the body is malformed.
                    return false;
                }
            }
        }

        static string FormatTime(DateTimeOffset value)
        {
            return value.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);
        }
    }
}
