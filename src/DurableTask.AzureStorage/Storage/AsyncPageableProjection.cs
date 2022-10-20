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
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;

    sealed class AsyncPageableProjection<TSource, TResult> : AsyncPageable<TResult>
        where TSource : notnull
        where TResult : notnull
    {
        readonly AsyncPageable<TSource> source;
        readonly Func<TSource, TResult> selector;

        public AsyncPageableProjection(AsyncPageable<TSource> source, Func<TSource, TResult> selector)
        {
            this.source = source ?? throw new ArgumentNullException(nameof(source));
            this.selector = selector ?? throw new ArgumentNullException(nameof(selector));
        }

        public override IAsyncEnumerable<Page<TResult>> AsPages(string? continuationToken = null, int? pageSizeHint = null) =>
            new AsyncPageableProjectionEnumerable(this.source.AsPages(continuationToken, pageSizeHint), this.selector);

        sealed class AsyncPageableProjectionEnumerable : IAsyncEnumerable<Page<TResult>>
        {
            readonly IAsyncEnumerable<Page<TSource>> source;
            readonly Func<TSource, TResult> selector;

            public AsyncPageableProjectionEnumerable(IAsyncEnumerable<Page<TSource>> source, Func<TSource, TResult> selector)
            {
                this.source = source;
                this.selector = selector;
            }

            public async IAsyncEnumerator<Page<TResult>> GetAsyncEnumerator(CancellationToken cancellationToken = default(CancellationToken))
            {
                await foreach (Page<TSource> page in this.source.WithCancellation(cancellationToken))
                {
                    var result = Page<TResult>.FromValues(page.Values.Select(this.selector).ToList(), page.ContinuationToken, page.GetRawResponse());
                    yield return result;
                }
            }
        }
    }
}
