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
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;

    static class AsyncPageableExtensions
    {
        public static async Task<Page<T>?> GetPageAsync<T>(this AsyncPageable<T> pageable, string? continuationToken = null, int? pageSizeHint = null, CancellationToken cancellationToken = default)
            where T : notnull
        {
            if (pageable == null)
            {
                throw new ArgumentNullException(nameof(pageable));
            }

            IAsyncEnumerator<Page<T>> asyncEnumerator = pageable.AsPages(continuationToken, pageSizeHint).GetAsyncEnumerator(cancellationToken);
            return await asyncEnumerator.MoveNextAsync() ? asyncEnumerator.Current : null;
        }
    }
}
