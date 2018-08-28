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
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;

    sealed class AsyncQueue<T> : IDisposable
    {
        readonly SemaphoreSlim semaphore = new SemaphoreSlim(0);
        readonly ConcurrentQueue<T> innerQueue = new ConcurrentQueue<T>();

        public int Count => this.innerQueue.Count;

        public void Enqueue(T item)
        {
            this.innerQueue.Enqueue(item);
            this.semaphore.Release();
        }

        public async Task<T> DequeueAsync(CancellationToken cancellationToken)
        {
            while (true)
            {
                await this.semaphore.WaitAsync(cancellationToken);

                if (this.innerQueue.TryDequeue(out T item))
                {
                    return item;
                }
            }
        }

        public void Dispose()
        {
            this.semaphore.Dispose();
        }
    }
}
