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

namespace DurableTask.AzureServiceFabric.Stores
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.ServiceFabric.Data;

    class ActivityProvider : MessageProviderBase<string, TaskMessageItem>
    {
        readonly ConcurrentQueue<string> inMemoryQueue = new ConcurrentQueue<string>();

        public ActivityProvider(IReliableStateManager stateManager, string storeName, CancellationToken token) : base(stateManager, storeName, token)
        {
        }

        protected override void AddItemInMemory(string key, TaskMessageItem value)
        {
            this.inMemoryQueue.Enqueue(key);
        }

        public async Task<Message<string, TaskMessageItem>> ReceiveAsync(TimeSpan receiveTimeout)
        {
            if (!IsStopped())
            {
                bool newItemsBeforeTimeout = true;
                while (newItemsBeforeTimeout)
                {
                    if (this.inMemoryQueue.TryDequeue(out string key))
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

        public void Abandon(string key)
        {
            this.inMemoryQueue.Enqueue(key);
            SetWaiterForNewItems();
        }
    }
}
