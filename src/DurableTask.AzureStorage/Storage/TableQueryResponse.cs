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
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using DurableTask.AzureStorage.Monitoring;

    class TableQueryResponse<T> : AsyncPageable<T> where T : notnull
    {
        readonly AsyncPageable<T> query;
        readonly AzureStorageOrchestrationServiceStats stats;

        public TableQueryResponse(AsyncPageable<T> query, AzureStorageOrchestrationServiceStats stats)
        {
            this.query = query ?? throw new ArgumentNullException(nameof(query));
            this.stats = stats ?? throw new ArgumentNullException(nameof(stats));
        }

        public override IAsyncEnumerable<Page<T>> AsPages(string? continuationToken = null, int? pageSizeHint = null)
        {
            return this.query.AsPages(continuationToken, pageSizeHint);
        }

        public async Task<TableQueryResults<T>> GetResultsAsync(
            string? continuationToken = null,
            int? pageSizeHint = null,
            CancellationToken cancellationToken = default)
        {
            var sw = Stopwatch.StartNew();

            int pages = 0;
            var entities = new List<T>();
            await foreach (Page<T> page in this.query.AsPages(continuationToken, pageSizeHint).WithCancellation(cancellationToken))
            {
                this.stats.TableEntitiesRead.Increment(page.Values.Count);

                pages++;
                entities.AddRange(page.Values);
            }

            sw.Stop();
            return new TableQueryResults<T>(entities, sw.Elapsed, pages);
        }
    }
}
