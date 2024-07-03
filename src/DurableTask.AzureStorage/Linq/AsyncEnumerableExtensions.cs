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
namespace DurableTask.AzureStorage.Linq
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;

    static class AsyncEnumerableExtensions
    {
        // Note: like Enumerable.Select, SelectAsync only supports projecting TSource to TResult
        public static async IAsyncEnumerable<TResult> SelectAsync<TSource, TResult>(
            this IEnumerable<TSource> source,
            Func<TSource, CancellationToken, ValueTask<TResult>> selectAsync,
            [EnumeratorCancellation] CancellationToken cancellationToken = default(CancellationToken))
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (selectAsync == null)
            {
                throw new ArgumentNullException(nameof(selectAsync));
            }

            foreach (TSource item in source)
            {
                yield return await selectAsync(item, cancellationToken);
            }
        }

        // Note: The TransformPages methods support potentially complex transformations
        // by using delegates that return IEnumerable<TResult> and IAsyncEnumerable<TResult>
        public static AsyncPageable<TResult> TransformPages<TSource, TResult>(this AsyncPageable<TSource> source, Func<Page<TSource>, IEnumerable<TResult>> transform)
            where TSource : notnull
            where TResult : notnull =>
            new AsyncPageableTransformation<TSource, TResult>(source, transform);

        public static AsyncPageable<TResult> TransformPagesAsync<TSource, TResult>(this AsyncPageable<TSource> source, Func<Page<TSource>, CancellationToken, IAsyncEnumerable<TResult>> transformAsync)
            where TSource : notnull
            where TResult : notnull =>
            new AsyncPageableAsyncTransformation<TSource, TResult>(source, transformAsync);

        sealed class AsyncPageableTransformation<TSource, TResult> : AsyncPageable<TResult>
            where TSource : notnull
            where TResult : notnull
        {
            readonly AsyncPageable<TSource> source;
            readonly Func<Page<TSource>, IEnumerable<TResult>> transform;

            public AsyncPageableTransformation(AsyncPageable<TSource> source, Func<Page<TSource>, IEnumerable<TResult>> transform)
            {
                this.source = source ?? throw new ArgumentNullException(nameof(source));
                this.transform = transform ?? throw new ArgumentNullException(nameof(transform));
            }

            public override IAsyncEnumerable<Page<TResult>> AsPages(string? continuationToken = null, int? pageSizeHint = null) =>
                new AsyncEnumerable(this.source.AsPages(continuationToken, pageSizeHint), this.transform);

            sealed class AsyncEnumerable : IAsyncEnumerable<Page<TResult>>
            {
                readonly IAsyncEnumerable<Page<TSource>> source;
                readonly Func<Page<TSource>, IEnumerable<TResult>> transform;

                public AsyncEnumerable(IAsyncEnumerable<Page<TSource>> source, Func<Page<TSource>, IEnumerable<TResult>> transform)
                {
                    this.source = source;
                    this.transform = transform;
                }

                public async IAsyncEnumerator<Page<TResult>> GetAsyncEnumerator(CancellationToken cancellationToken = default(CancellationToken))
                {
                    await foreach (Page<TSource> page in this.source.WithCancellation(cancellationToken))
                    {
                        yield return Page<TResult>.FromValues(
                            this.transform(page).ToList(),
                            page.ContinuationToken,
                            page.GetRawResponse());
                    }
                }
            }
        }

        sealed class AsyncPageableAsyncTransformation<TSource, TResult> : AsyncPageable<TResult>
            where TSource : notnull
            where TResult : notnull
        {
            readonly AsyncPageable<TSource> source;
            readonly Func<Page<TSource>, CancellationToken, IAsyncEnumerable<TResult>> transformAsync;

            public AsyncPageableAsyncTransformation(AsyncPageable<TSource> source, Func<Page<TSource>, CancellationToken, IAsyncEnumerable<TResult>> transformAsync)
            {
                this.source = source ?? throw new ArgumentNullException(nameof(source));
                this.transformAsync = transformAsync ?? throw new ArgumentNullException(nameof(transformAsync));
            }

            public override IAsyncEnumerable<Page<TResult>> AsPages(string? continuationToken = null, int? pageSizeHint = null) =>
                new AsyncEnumerable(this.source.AsPages(continuationToken, pageSizeHint), this.transformAsync);

            sealed class AsyncEnumerable : IAsyncEnumerable<Page<TResult>>
            {
                readonly IAsyncEnumerable<Page<TSource>> source;
                readonly Func<Page<TSource>, CancellationToken, IAsyncEnumerable<TResult>> transformAsync;

                public AsyncEnumerable(IAsyncEnumerable<Page<TSource>> source, Func<Page<TSource>, CancellationToken, IAsyncEnumerable<TResult>> transformAsync)
                {
                    this.source = source;
                    this.transformAsync = transformAsync;
                }

                public async IAsyncEnumerator<Page<TResult>> GetAsyncEnumerator(CancellationToken cancellationToken = default(CancellationToken))
                {
                    await foreach (Page<TSource> page in this.source.WithCancellation(cancellationToken))
                    {
                        yield return Page<TResult>.FromValues(
                            await this.transformAsync(page, cancellationToken).ToListAsync(cancellationToken),
                            page.ContinuationToken,
                            page.GetRawResponse());
                    }
                }
            }
        }
    }
}
