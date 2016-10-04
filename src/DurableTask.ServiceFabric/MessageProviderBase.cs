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
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Data;
    using Microsoft.ServiceFabric.Data.Collections;

    abstract class MessageProviderBase<TKey, TValue> where TKey : IComparable<TKey>, IEquatable<TKey>
    {
        readonly IReliableStateManager stateManager;
        readonly string storeName;
        readonly CancellationTokenSource cancellationTokenSource;
        readonly AsyncManualResetEvent waitEvent = new AsyncManualResetEvent();

        IReliableDictionary<TKey, TValue> store;

        protected MessageProviderBase(IReliableStateManager stateManager, string storeName)
        {
            this.stateManager = stateManager;
            this.storeName = storeName;
            this.cancellationTokenSource = new CancellationTokenSource();
        }

        public async Task StartAsync()
        {
            this.store = await this.stateManager.GetOrAddAsync<IReliableDictionary<TKey, TValue>>(this.storeName);

            using (var tx = this.stateManager.CreateTransaction())
            {
                var count = await this.store.GetCountAsync(tx);

                if (count > 0)
                {
                    var enumerable = await this.store.CreateEnumerableAsync(tx, EnumerationMode.Unordered);
                    using (var enumerator = enumerable.GetAsyncEnumerator())
                    {
                        while (await enumerator.MoveNextAsync(this.cancellationTokenSource.Token))
                        {
                            AddItemInMemory(enumerator.Current.Key, enumerator.Current.Value);
                        }
                    }
                }
            }
        }

        public Task StopAsync()
        {
            this.cancellationTokenSource.Cancel();
            return Task.FromResult<object>(null);
        }

        public Task CompleteAsync(ITransaction tx, TKey key)
        {
            ThrowIfStopped();

            return this.store.TryRemoveAsync(tx, key);
        }

        /// <summary>
        /// Caller has to pass in a transaction and must call SendComplete with the same item
        /// after the transaction commit succeeded.
        /// </summary>
        public Task SendBeginAsync(ITransaction tx, Message<TKey, TValue> item)
        {
            ThrowIfStopped();

            return this.store.TryAddAsync(tx, item.Key, item.Value);
        }

        public void SendComplete(Message<TKey, TValue> item)
        {
            ThrowIfStopped();

            AddItemInMemory(item.Key, item.Value);
            this.waitEvent.Set();
        }

        /// <summary>
        /// Caller has to pass in a transaction and must call SendBatchComplete with the same items
        /// after the transaction commit succeeded.
        /// </summary>
        public async Task SendBatchBeginAsync(ITransaction tx, IEnumerable<Message<TKey, TValue>> items)
        {
            ThrowIfStopped();

            foreach (var item in items)
            {
                await this.store.TryAddAsync(tx, item.Key, item.Value);
            }
        }

        public void SendBatchComplete(IEnumerable<Message<TKey, TValue>> items)
        {
            ThrowIfStopped();

            foreach (var item in items)
            {
                AddItemInMemory(item.Key, item.Value);
            }
            this.waitEvent.Set();
        }

        protected async Task<Message<TKey, TValue>> GetValueAsync(TKey key)
        {
            using (var tx = this.stateManager.CreateTransaction())
            {
                var result = await this.store.TryGetValueAsync(tx, key);
                if (result.HasValue)
                {
                    return new Message<TKey, TValue>(key, result.Value);
                }
                throw new Exception("Internal server error : Unexpectedly ended up not having an item in dictionary while having the item key in memory");
            }
        }

        protected Task<bool> WaitForItemsAsync(TimeSpan timeout)
        {
            this.waitEvent.Reset();
            return this.waitEvent.WaitAsync(timeout, this.cancellationTokenSource.Token);
        }

        protected void SetWaiterForNewItems()
        {
            this.waitEvent.Set();
        }

        protected abstract void AddItemInMemory(TKey key, TValue value);

        protected void ThrowIfStopped()
        {
            this.cancellationTokenSource.Token.ThrowIfCancellationRequested();
        }
    }
}
