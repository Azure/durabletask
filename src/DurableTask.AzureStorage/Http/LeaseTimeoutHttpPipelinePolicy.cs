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
    using Azure.Core;
    using Azure.Core.Pipeline;

    sealed class LeaseTimeoutHttpPipelinePolicy : HttpPipelinePolicy
    {
        readonly TimeSpan leaseRenewalTimeout;

        public LeaseTimeoutHttpPipelinePolicy(TimeSpan leaseRenewalTimeout)
        {
            this.leaseRenewalTimeout = leaseRenewalTimeout;
        }

        public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            if (IsBlobLeaseRenewal(message.Request.Uri.Query, message.Request.Headers))
            {
                message.NetworkTimeout = this.leaseRenewalTimeout;
            }

            ProcessNext(message, pipeline);
        }

        public override ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            if (IsBlobLeaseRenewal(message.Request.Uri.Query, message.Request.Headers))
            {
                message.NetworkTimeout = this.leaseRenewalTimeout;
            }

            return ProcessNextAsync(message, pipeline);
        }

        static bool IsBlobLeaseRenewal(string query, RequestHeaders headers) =>
            query.IndexOf("comp=lease", StringComparison.OrdinalIgnoreCase) != -1 &&
            headers.TryGetValue("x-ms-lease-action", out string? value) &&
            string.Equals(value, "renew", StringComparison.OrdinalIgnoreCase);
    }
}
