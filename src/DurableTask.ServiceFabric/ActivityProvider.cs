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

namespace DurableTask.ServiceFabric
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Data;

    class ActivityProvider<TKey, TValue> : MessageProviderBase<TKey, TValue> where TKey : IComparable<TKey>, IEquatable<TKey>
    {
        readonly ConcurrentQueue<TKey> inMemoryQueue = new ConcurrentQueue<TKey>();

        public ActivityProvider(IReliableStateManager stateManager, string storeName) : base(stateManager, storeName)
        {
        }

        protected override void AddItemInMemory(TKey key, TValue value)
        {
            this.inMemoryQueue.Enqueue(key);
        }

        public async Task<Message<TKey, TValue>> ReceiveAsync(TimeSpan receiveTimeout)
        {
            if (!IsStopped())
            {
                TKey key;
                bool newItemsBeforeTimeout = true;
                while (newItemsBeforeTimeout)
                {
                    if (this.inMemoryQueue.TryDequeue(out key))
                    {
                        try
                        {
                            return await GetValueAsync(key);
                        }
                        catch (Exception)
                        {
                            this.inMemoryQueue.Enqueue(key);
                            throw;
                        }
                    }

                    newItemsBeforeTimeout = await WaitForItemsAsync(receiveTimeout);
                }
            }
            return null;
        }

        public void Abandon(TKey key)
        {
            ThrowIfStopped();

            this.inMemoryQueue.Enqueue(key);
            SetWaiterForNewItems();
        }
    }
}
