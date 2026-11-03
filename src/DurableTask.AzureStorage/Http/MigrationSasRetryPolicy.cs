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
            if (IsAuthenticationFailedResponse(message))
            {
                this.manager.RefreshAsync(message.CancellationToken).GetAwaiter().GetResult();
                this.manager.EnsureAccess();
                message.CancellationToken.ThrowIfCancellationRequested();
                message.Response.Dispose();
                // Run authentication again with the refreshed credential. Retry only once.
                ProcessNext(message, pipeline);
            }
        }

        public async override ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            await ProcessNextAsync(message, pipeline).ConfigureAwait(false);
            if (IsAuthenticationFailedResponse(message))
            {
                await this.manager.RefreshAsync(message.CancellationToken).ConfigureAwait(false);
                this.manager.EnsureAccess();
                message.CancellationToken.ThrowIfCancellationRequested();
                message.Response.Dispose();
                await ProcessNextAsync(message, pipeline).ConfigureAwait(false);
            }
        }

        static bool IsAuthenticationFailedResponse(HttpMessage message)
        {
            Response response = message.Response;

            // We intentionally keep our retry check relatively broad, which could mean that we retry unrelated errors,
            // but we accept this cost because we only retry once anyway
            return response.Status == 403 &&
                response.Headers.TryGetValue("x-ms-error-code", out string? code) && code == "AuthenticationFailed";
        }
    }
}
