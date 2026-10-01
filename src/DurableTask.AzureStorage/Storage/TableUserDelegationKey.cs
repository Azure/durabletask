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
        readonly Dictionary<string, string> fields;
        readonly byte[] key;

        TableUserDelegationKey(XElement xml)
        {
            this.fields = new Dictionary<string, string>
            {
                ["skoid"] = Read("SignedOid"), ["sktid"] = Read("SignedTid"),
                ["skt"] = Read("SignedStart"), ["ske"] = Read("SignedExpiry"),
                ["sks"] = Read("SignedService"), ["skv"] = Read("SignedVersion"),
            };
            if (this.fields["sks"] != "t") throw new InvalidOperationException("Expected a Table user delegation key.");
            this.key = Convert.FromBase64String(Read("Value"));
            this.ExpiresOn = DateTimeOffset.Parse(this.fields["ske"], CultureInfo.InvariantCulture);
            string Read(string name) => (string?)xml.Element(name) ?? throw new InvalidOperationException("Missing user delegation key field: " + name);
        }

        public DateTimeOffset ExpiresOn { get; }

        public static async Task<TableUserDelegationKey> GetAsync(Uri endpoint, TokenCredential credential, TableClientOptions options, DateTimeOffset serverTime, CancellationToken cancellationToken)
        {
            string audience = (options.Audience ?? TableAudience.AzurePublicCloud).ToString().TrimEnd('/');
            // Workload options may contain the SAS fence policy. Key acquisition has its own authenticated
            // pipeline and cannot depend on an existing, unexpired workload SAS.
            var keyOptions = new TableClientOptions { Transport = options.Transport };
            keyOptions.Retry.Mode = options.Retry.Mode;
            keyOptions.Retry.Delay = options.Retry.Delay;
            keyOptions.Retry.MaxDelay = options.Retry.MaxDelay;
            keyOptions.Retry.MaxRetries = options.Retry.MaxRetries;
            keyOptions.Retry.NetworkTimeout = options.Retry.NetworkTimeout;
            HttpPipeline pipeline = HttpPipelineBuilder.Build(keyOptions, new BearerTokenAuthenticationPolicy(credential, audience + "/.default"));
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
            if (message.Response.Status != 200) throw new RequestFailedException(message.Response);
            using var reader = new StreamReader(message.Response.ContentStream!);
            return new TableUserDelegationKey(XElement.Parse(await reader.ReadToEndAsync().ConfigureAwait(false)));
        }

        public string Sign(string accountName, string tableName, DateTimeOffset expiresOn)
        {
            string start = this.fields["skt"];
            string expiry = FormatTime(expiresOn);
            string permissions = "raud";
            // Empty delegated tenant/user, IP, and partition/row ranges are required positions in the signature.
            string toSign = string.Join("\n", permissions, start, expiry,
                "/table/" + accountName + "/" + tableName.ToLowerInvariant(),
                this.fields["skoid"], this.fields["sktid"], this.fields["skt"], this.fields["ske"],
                this.fields["sks"], this.fields["skv"], "", "", "", "https", ServiceVersion, "", "", "", "");
            using var hmac = new HMACSHA256(this.key);
            var query = new Dictionary<string, string>(this.fields)
            {
                ["sp"] = permissions, ["st"] = start, ["se"] = expiry,
                ["spr"] = "https", ["sv"] = ServiceVersion, ["tn"] = tableName,
                ["sig"] = Convert.ToBase64String(hmac.ComputeHash(Encoding.UTF8.GetBytes(toSign))),
            };
            return string.Join("&", query.Select(pair => pair.Key + "=" + Uri.EscapeDataString(pair.Value)));
        }

        static string FormatTime(DateTimeOffset value) => value.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);
    }
}
