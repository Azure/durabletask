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
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;

    static class AsyncPageable
    {
        public static AsyncPageable<T> FromAsyncPage<T>(Func<CancellationToken, Task<Page<T>>> getPageAsync, CancellationToken cancellationToken = default)
        {
            return new SingletonAsyncPageable<T>(getPageAsync, cancellationToken);
        }

        sealed class SingletonAsyncPageable<T> : AsyncPageable<T>
        {
            readonly Func<CancellationToken, Task<Page<T>>> _getPageAsync;

            public SingletonAsyncPageable(Func<CancellationToken, Task<Page<T>>> getPageAsync, CancellationToken cancellationToken)
                : base(cancellationToken)
            {
                this._getPageAsync = getPageAsync ?? throw new ArgumentNullException(nameof(getPageAsync));
            }

            public override async IAsyncEnumerable<Page<T>> AsPages(string continuationToken = null, int? pageSizeHint = null)
            {
                Page<T> page = await this._getPageAsync(this.CancellationToken);
                if (page != null)
                {
                    yield return page;
                }
            }
        }
    }
}
