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
    using System.Collections.Generic;
    using System.Runtime.Serialization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Data;
    using Microsoft.ServiceFabric.Data.Collections;

    class PeekLockQueue<TKey, TValue> where TKey : IComparable<TKey>, IEquatable<TKey>
    {
        readonly IReliableStateManager stateManager;
        readonly string storeName;
        readonly CancellationTokenSource cancellationTokenSource;

        IReliableDictionary<TKey, TValue> store;
        readonly ConcurrentQueue<TKey> inMemoryQueue = new ConcurrentQueue<TKey>();

        readonly AsyncManualResetEvent waitEvent = new AsyncManualResetEvent();

        public PeekLockQueue(IReliableStateManager stateManager, string storeName)
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
                            this.inMemoryQueue.Enqueue(enumerator.Current.Key);
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

        public async Task<Message<TKey, TValue>> ReceiveAsync(TimeSpan receiveTimeout)
        {
            ThrowIfStopped();

            TKey key;
            bool newItemsBeforeTimeout = true;
            while (newItemsBeforeTimeout)
            {
                if (this.inMemoryQueue.TryDequeue(out key))
                {
                    try
                    {
                        using (var txn = this.stateManager.CreateTransaction())
                        {
                            var result = await this.store.TryGetValueAsync(txn, key);
                            if (result.HasValue)
                            {
                                return new Message<TKey, TValue>(key, result.Value);
                            }
                            throw new Exception("Internal server error : Unexpectedly ended up not having an item in dictionary while having the item key in memory");
                        }
                    }
                    catch (TimeoutException)
                    {
                        this.inMemoryQueue.Enqueue(key);
                        throw;
                    }
                }

                this.waitEvent.Reset();
                newItemsBeforeTimeout = await this.waitEvent.WaitAsync(receiveTimeout, this.cancellationTokenSource.Token);
            }

            return null;
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

            this.inMemoryQueue.Enqueue(item.Key);
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
                this.inMemoryQueue.Enqueue(item.Key);
            }
            this.waitEvent.Set();
        }

        public Task AandonAsync(TKey key)
        {
            ThrowIfStopped();

            this.inMemoryQueue.Enqueue(key);
            this.waitEvent.Set();
            return Task.FromResult<object>(null);
        }

        void ThrowIfStopped()
        {
            this.cancellationTokenSource.Token.ThrowIfCancellationRequested();
        }
    }

    [DataContract]
    class Message<TKey, TValue> where TKey : IComparable<TKey>, IEquatable<TKey>
    {
        public Message(TKey key, TValue value)
        {
            Key = key;
            Value = value;
        }

        [DataMember]
        public TKey Key { get; private set; }

        [DataMember]
        public TValue Value { get; private set; }
    }
}
