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
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Fabric;
    using System.Threading;
    using System.Threading.Tasks;

    using DurableTask.AzureServiceFabric.TaskHelpers;
    using DurableTask.AzureServiceFabric.Tracing;

    using Microsoft.ServiceFabric.Data;
    using Microsoft.ServiceFabric.Data.Collections;

    [SuppressMessage("Microsoft.Design", "CA1001", Justification = "Disposing is done through StartAsync and StopAsync")]
    abstract class MessageProviderBase<TKey, TValue> where TKey : IComparable<TKey>, IEquatable<TKey>
    {
        readonly string storeName;
        readonly AsyncManualResetEvent waitEvent = new AsyncManualResetEvent();
        readonly TimeSpan metricsInterval = TimeSpan.FromMinutes(1);

        protected MessageProviderBase(IReliableStateManager stateManager, string storeName, CancellationToken token)
        {
            this.StateManager = stateManager;
            this.storeName = storeName;
            this.CancellationToken = token;
        }

        protected IReliableStateManager StateManager { get; }

        protected CancellationToken CancellationToken { get; private set; }

        protected IReliableDictionary<TKey, TValue> Store { get; private set; }

        public virtual async Task StartAsync()
        {
            await this.InitializeStore();
            await this.EnumerateItems(kvp => this.AddItemInMemory(kvp.Key, kvp.Value));
            Task nowait = this.LogMetrics();
        }

        protected async Task InitializeStore()
        {
            this.Store = await this.StateManager.GetOrAddAsync<IReliableDictionary<TKey, TValue>>(this.storeName);
        }

        public Task CompleteAsync(ITransaction tx, TKey key)
        {
            return this.Store.TryRemoveAsync(tx, key);
        }

        public async Task CompleteBatchAsync(ITransaction tx, IEnumerable<TKey> keys)
        {
            foreach (TKey key in keys)
            {
                await this.Store.TryRemoveAsync(tx, key);
            }
        }

        /// <summary>
        /// Caller has to pass in a transaction and must call SendComplete with the same item
        /// after the transaction commit succeeded.
        /// </summary>
        public Task SendBeginAsync(ITransaction tx, Message<TKey, TValue> item)
        {
            return this.Store.TryAddAsync(tx, item.Key, item.Value);
        }

        public void SendComplete(Message<TKey, TValue> item)
        {
            this.AddItemInMemory(item.Key, item.Value);
            this.waitEvent.Set();
        }

        /// <summary>
        /// Caller has to pass in a transaction and must call SendBatchComplete with the same items
        /// after the transaction commit succeeded.
        /// </summary>
        public async Task SendBatchBeginAsync(ITransaction tx, IEnumerable<Message<TKey, TValue>> items)
        {
            foreach (Message<TKey, TValue> item in items)
            {
                await this.Store.TryAddAsync(tx, item.Key, item.Value);
            }
        }

        public void SendBatchComplete(IEnumerable<Message<TKey, TValue>> items)
        {
            foreach (Message<TKey, TValue> item in items)
            {
                this.AddItemInMemory(item.Key, item.Value);
            }
            this.waitEvent.Set();
        }

        protected Task<Message<TKey, TValue>> GetValueAsync(TKey key)
        {
            return RetryHelper.ExecuteWithRetryOnTransient(async () =>
            {
                using (var tx = this.StateManager.CreateTransaction())
                {
                    var result = await this.Store.TryGetValueAsync(tx, key);
                    if (result.HasValue)
                    {
                        return new Message<TKey, TValue>(key, result.Value);
                    }
                    var errorMessage = $"Internal Server Error: Did not find an item in reliable dictionary while having the item key {key} in memory";
                    ServiceFabricProviderEventSource.Tracing.UnexpectedCodeCondition(errorMessage);
                    throw new Exception(errorMessage);
                }
            }, uniqueActionIdentifier: $"Key = {key}, Action = MessageProviderBase.GetValueAsync, StoreName : {this.storeName}");
        }

        protected Task EnumerateItems(Action<KeyValuePair<TKey, TValue>> itemAction)
        {
            return RetryHelper.ExecuteWithRetryOnTransient(async () =>
            {
                using (var tx = this.StateManager.CreateTransaction())
                {
                    var count = await this.Store.GetCountAsync(tx);

                    if (count > 0)
                    {
                        var enumerable = await this.Store.CreateEnumerableAsync(tx, EnumerationMode.Unordered);
                        using (var enumerator = enumerable.GetAsyncEnumerator())
                        {
                            while (await enumerator.MoveNextAsync(this.CancellationToken))
                            {
                                itemAction(enumerator.Current);
                            }
                        }
                    }
                }
            }, uniqueActionIdentifier: $"Action = MessageProviderBase.EnumerateItems, StoreName : {this.storeName}");
        }

        public Task EnsureStoreInitialized()
        {
            if (this.Store == null)
            {
                return this.InitializeStore();
            }

            return Task.CompletedTask;
        }

        protected Task<bool> WaitForItemsAsync(TimeSpan timeout)
        {
            this.waitEvent.Reset();
            return this.waitEvent.WaitAsync(timeout, this.CancellationToken);
        }

        protected void SetWaiterForNewItems()
        {
            this.waitEvent.Set();
        }

        protected abstract void AddItemInMemory(TKey key, TValue value);

        protected bool IsStopped()
        {
            return this.CancellationToken.IsCancellationRequested;
        }

        protected Task LogMetrics()
        {
            return Utils.RunBackgroundJob(async () =>
            {
                try
                {
                    using (ITransaction tx = this.StateManager.CreateTransaction())
                    {
                        long count = await this.Store.GetCountAsync(tx);
                        ServiceFabricProviderEventSource.Tracing.LogStoreCount(this.storeName, count);
                    }
                }
                catch (FabricObjectClosedException)
                {
                    ServiceFabricProviderEventSource.Tracing.ExceptionWhileRunningBackgroundJob("LogMetrics", "Fabric object is closed while running the loop action");
                }
            }, initialDelay: this.metricsInterval, delayOnSuccess: this.metricsInterval, delayOnException: this.metricsInterval, actionName: $"Log Store Count of {this.storeName}", token: this.CancellationToken);
        }
    }
}
