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
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Core;
    using Azure.Core.Pipeline;

    sealed class ThrottlingHttpPipelinePolicy : HttpPipelinePolicy, IDisposable
    {
        readonly SemaphoreSlim throttle;

        public ThrottlingHttpPipelinePolicy(int maxRequests)
        {
            this.throttle = new SemaphoreSlim(maxRequests);
        }

        public void Dispose()
        {
            this.throttle.Dispose();
        }

        public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            if (IsBlobLease(message.Request.Uri.Query))
            {
                ProcessNext(message, pipeline);
            }
            else
            {
                this.throttle.Wait(message.CancellationToken);
                try
                {
                    ProcessNext(message, pipeline);
                }
                finally
                {
                    this.throttle.Release();
                }
            }
        }

        public override ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline) =>
            IsBlobLease(message.Request.Uri.Query) ? ProcessNextAsync(message, pipeline) : this.ThrottledProcessNextAsync(message, pipeline);

        async ValueTask ThrottledProcessNextAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            await this.throttle.WaitAsync(message.CancellationToken);
            try
            {
                await ProcessNextAsync(message, pipeline);
            }
            finally
            {
                this.throttle.Release();
            }
        }

        static bool IsBlobLease(string query) =>
            query.IndexOf("comp=lease", StringComparison.OrdinalIgnoreCase) != -1;
    }
}
