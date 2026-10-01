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
    using System.Linq;
    using Azure.Core;
    using Azure.Core.Pipeline;
    using DurableTask.AzureStorage.Storage;

    // Installed only on SAS workload clients; runs again for every SDK retry, including paged reads and blob streams.
    sealed class MigrationRequestPolicy : HttpPipelineSynchronousPolicy
    {
        readonly MigrationSasManager manager;
        readonly bool useDelegationVersion;

        public MigrationRequestPolicy(MigrationSasManager manager, bool useDelegationVersion = false)
        {
            this.manager = manager;
            this.useDelegationVersion = useDelegationVersion;
        }

        public override void OnSendingRequest(HttpMessage message)
        {
            this.manager.EnsureAccess();
            // SDK retries reuse the URI. Replace the parameter rather than appending duplicates.
            var uri = new UriBuilder(message.Request.Uri.ToUri());
            string query = string.Join("&", uri.Query.TrimStart('?').Split('&')
                .Where(part => part.Length > 0 && !part.StartsWith("timeout=", StringComparison.OrdinalIgnoreCase)));
            uri.Query = query.Length == 0 ? "timeout=30" : query + "&timeout=30";
            message.Request.Uri.Reset(uri.Uri);
            if (this.useDelegationVersion)
            { 
                message.Request.Headers.SetValue("x-ms-version", "2025-07-05");
            }
        }
    }
}
