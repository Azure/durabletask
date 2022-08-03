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

namespace DurableTask.AzureStorage
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;

    sealed class AsyncPageableProjection<TSource, TResult> : AsyncPageable<TResult>
    {
        readonly AsyncPageable<TSource> _source;
        readonly Func<TSource, CancellationToken, Task<TResult>> _selectorAsync;

        public AsyncPageableProjection(AsyncPageable<TSource> source, Func<TSource, CancellationToken, Task<TResult>> selectorAsync)
        {
            this._source = source ?? throw new ArgumentNullException(nameof(source));
            this._selectorAsync = selectorAsync ?? throw new ArgumentNullException(nameof(selectorAsync));
        }

        public override IAsyncEnumerable<Page<TResult>> AsPages(string continuationToken = null, int? pageSizeHint = null)
        {
            return this._source
                .AsPages(continuationToken, pageSizeHint)
                .SelectAwaitWithCancellation(async (x, t) =>
                {
                    var newValues = new List<TResult>();
                    foreach (TSource v in x.Values)
                    {
                        newValues.Add(await this._selectorAsync(v, t));
                    }

                    return Page<TResult>.FromValues(newValues, x.ContinuationToken, x.GetRawResponse());
                });
        }
    }
}
