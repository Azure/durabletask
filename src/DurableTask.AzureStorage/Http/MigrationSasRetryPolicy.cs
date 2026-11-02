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
namespace DurableTask.AzureStorage.Http
{
    using System;
    using System.Globalization;
    using System.Linq;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Core;
    using Azure.Core.Pipeline;
    using DurableTask.AzureStorage.Storage;

    sealed class MigrationSasRetryPolicy : HttpPipelinePolicy
    {
        readonly MigrationSasManager manager;

        public MigrationSasRetryPolicy(MigrationSasManager manager)
        {
            this.manager = manager;
        }

        public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            ProcessNext(message, pipeline);
            if (IsExpiredSasResponse(message))
            {
                this.manager.RefreshAsync(message.CancellationToken).GetAwaiter().GetResult();
                this.manager.EnsureAccess();
                message.CancellationToken.ThrowIfCancellationRequested();
                message.Response.Dispose();
                // Run authentication again with the refreshed credential. Retry only once.
                ProcessNext(message, pipeline);
            }
        }

        public override async ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            await ProcessNextAsync(message, pipeline).ConfigureAwait(false);
            if (IsExpiredSasResponse(message))
            {
                await this.manager.RefreshAsync(message.CancellationToken).ConfigureAwait(false);
                this.manager.EnsureAccess();
                message.CancellationToken.ThrowIfCancellationRequested();
                message.Response.Dispose();
                await ProcessNextAsync(message, pipeline).ConfigureAwait(false);
            }
        }

        static bool IsExpiredSasResponse(HttpMessage message)
        {
            Response response = message.Response;
            if (response.Status != 403 ||
                !response.Headers.TryGetValue("x-ms-error-code", out string? code) || code != "AuthenticationFailed" ||
                response.Headers.Date is not DateTimeOffset serverTime)
            {
                return false;
            }

            // Check the failed request's expiry against Storage's clock, not the host clock.
            string? expiry = message.Request.Uri.Query.TrimStart('?').Split('&')
                .FirstOrDefault(part => part.StartsWith("se=", StringComparison.Ordinal));
            return expiry != null &&
                DateTimeOffset.TryParse(Uri.UnescapeDataString(expiry.Substring(3)), CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal, out DateTimeOffset expiresAt) && serverTime > expiresAt &&
                new RequestFailedException(response).Message.IndexOf("Signature not valid in the specified time frame", StringComparison.Ordinal) >= 0;
        }
    }
}
