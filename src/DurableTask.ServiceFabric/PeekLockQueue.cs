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
    using System.Diagnostics;
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
        ConcurrentQueue<TKey> inMemoryQueue = new ConcurrentQueue<TKey>();

        public PeekLockQueue(IReliableStateManager stateManager, string storeName)
        {
            this.stateManager = stateManager;
            this.storeName = storeName;
            this.cancellationTokenSource = new CancellationTokenSource();
        }

        public async Task StartAsync()
        {
            this.store = await this.stateManager.GetOrAddAsync<IReliableDictionary<TKey, TValue>>(this.storeName);

            using (var txn = this.stateManager.CreateTransaction())
            {
                //Todo: Check count first
                var enumerable = await this.store.CreateEnumerableAsync(txn, EnumerationMode.Unordered);
                using (var enumerator = enumerable.GetAsyncEnumerator())
                {
                    while (await enumerator.MoveNextAsync(this.cancellationTokenSource.Token))
                    {
                        this.inMemoryQueue.Enqueue(enumerator.Current.Key);
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
            Stopwatch timer = Stopwatch.StartNew();
            while (timer.Elapsed < receiveTimeout && !this.cancellationTokenSource.IsCancellationRequested)
            {
                TKey activityId;
                if (this.inMemoryQueue.TryDequeue(out activityId))
                {
                    using (var txn = this.stateManager.CreateTransaction())
                    {
                        var activity = await this.store.TryGetValueAsync(txn, activityId);
                        if (activity.HasValue)
                        {
                            return new Message<TKey, TValue>(activityId, activity.Value);
                        }
                        throw new Exception("Internal server error");
                    }
                }

                //Todo : Can use a signal mechanism between send and receive
                await Task.Delay(100, this.cancellationTokenSource.Token);
            }
            return null;
        }

        public Task CompleteAsync(ITransaction tx, TKey key)
        {
            return this.store.TryRemoveAsync(tx, key);
        }

        /// <summary>
        /// Caller has to pass in a transaction and must call SendComplete with the same item
        /// after the transaction commit succeeded.
        /// </summary>
        public Task SendBeginAsync(ITransaction tx, Message<TKey, TValue> item)
        {
            return this.store.TryAddAsync(tx, item.Key, item.Value);
        }

        public void SendComplete(Message<TKey, TValue> item)
        {
            this.inMemoryQueue.Enqueue(item.Key);
        }

        /// <summary>
        /// Caller has to pass in a transaction and must call SendBatchComplete with the same items
        /// after the transaction commit succeeded.
        /// </summary>
        public async Task SendBatchBeginAsync(ITransaction tx, IEnumerable<Message<TKey, TValue>> items)
        {
            foreach (var item in items)
            {
                await this.SendBeginAsync(tx, item);
            }
        }

        public void SendBatchComplete(IEnumerable<Message<TKey, TValue>> items)
        {
            foreach (var item in items)
            {
                this.SendComplete(item);
            }
        }

        public Task AandonAsync(TKey key)
        {
            this.inMemoryQueue.Enqueue(key);
            return Task.FromResult<object>(null);
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
