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

    class TableQueryResponse<T> : IAsyncEnumerable<T> where T : notnull
    {
        readonly AsyncPageable<T> _query;

        public TableQueryResponse(AsyncPageable<T> query)
        {
            this._query = query ?? throw new ArgumentNullException(nameof(query));
        }

        public static implicit operator AsyncPageable<T>(TableQueryResponse<T> response) => response._query;

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return this._query.GetAsyncEnumerator(cancellationToken);
        }

        public async Task<TableQueryResults<T>> GetResultsAsync(string? continuationToken = null, int? pageSizeHint = null, CancellationToken cancellationToken = default)
        {
            var sw = new Stopwatch();
            sw.Start();

            int pages = 0;
            var entities = new List<T>();
            await foreach (Page<T> page in this._query.AsPages(continuationToken, pageSizeHint).WithCancellation(cancellationToken))
            {
                pages++;
                entities.AddRange(page.Values);
            }

            sw.Stop();
            return new TableQueryResults<T>(entities, sw.Elapsed, pages);
        }
    }
}
