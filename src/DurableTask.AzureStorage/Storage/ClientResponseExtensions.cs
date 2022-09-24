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
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;

    static class ClientResponseExtensions
    {
        public static AsyncPageable<T> DecorateFailure<T>(this AsyncPageable<T> paginatedResponse) where T : notnull =>
            new DecoratedAsyncPageable<T>(paginatedResponse);

        public static async Task<Response> DecorateFailure(this Task<Response> responseTask)
        {
            try
            {
                return await responseTask;
            }
            catch (RequestFailedException rfe)
            {
                throw new DurableTaskStorageException(rfe);
            }
        }

        public static async Task<Response<T>> DecorateFailure<T>(this Task<Response<T>> responseTask)
        {
            try
            {
                return await responseTask;
            }
            catch (RequestFailedException rfe)
            {
                throw new DurableTaskStorageException(rfe);
            }
        }

        sealed class DecoratedAsyncPageable<T> : AsyncPageable<T> where T : notnull
        {
            readonly AsyncPageable<T> source;

            public DecoratedAsyncPageable(AsyncPageable<T> source) =>
                this.source = source;

            public override IAsyncEnumerable<Page<T>> AsPages(string? continuationToken = null, int? pageSizeHint = null) =>
                new DecoratedPageEnumerable(this.source.AsPages(continuationToken, pageSizeHint));

            sealed class DecoratedPageEnumerable : IAsyncEnumerable<Page<T>>
            {
                readonly IAsyncEnumerable<Page<T>> source;

                public DecoratedPageEnumerable(IAsyncEnumerable<Page<T>> source) =>
                    this.source = source;

                public IAsyncEnumerator<Page<T>> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
                    new DecoratedPageEnumerator(this.source.GetAsyncEnumerator(cancellationToken));

                sealed class DecoratedPageEnumerator : IAsyncEnumerator<Page<T>>
                {
                    readonly IAsyncEnumerator<Page<T>> source;

                    public DecoratedPageEnumerator(IAsyncEnumerator<Page<T>> source) =>
                        this.source = source;

                    public Page<T> Current => this.source.Current;

                    public ValueTask DisposeAsync() =>
                        this.source.DisposeAsync();

                    public async ValueTask<bool> MoveNextAsync()
                    {
                        try
                        {
                            return await this.source.MoveNextAsync();
                        }
                        catch (RequestFailedException rfe)
                        {
                            throw new DurableTaskStorageException(rfe);
                        }
                    }
                }
            }
        }
    }
}
