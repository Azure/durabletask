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
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Data;
    using Microsoft.ServiceFabric.Data.Collections;

    //Todo: Refactor common components of this class and SessionsProvider
    class ActivitiesProvider
    {
        readonly IReliableStateManager stateManager;
        readonly CancellationTokenSource cancellationTokenSource;

        IReliableDictionary<string, TaskMessage> activityQueue;
        ConcurrentQueue<string> inMemoryQueue = new ConcurrentQueue<string>();
        ConcurrentDictionary<string, bool> lockTable = new ConcurrentDictionary<string, bool>();
        //bool newMessagesInStore;
        //object syncLock = new object();

        public ActivitiesProvider(IReliableStateManager stateManager)
        {
            if (stateManager == null)
            {
                throw new ArgumentNullException(nameof(stateManager));
            }

            this.stateManager = stateManager;
            this.cancellationTokenSource = new CancellationTokenSource();
        }

        public async Task StartAsync()
        {
            this.activityQueue = await this.stateManager.GetOrAddAsync<IReliableDictionary<string, TaskMessage>>(Constants.ActivitiesQueueName);
            //lock (syncLock)
            //{
            //    this.newMessagesInStore = true;
            //}
        }

        public void Stop()
        {
            this.cancellationTokenSource.Cancel();
        }

        public async Task<TaskMessage> GetNextWorkItem(TimeSpan receiveTimeout)
        {
            Stopwatch timer = Stopwatch.StartNew();
            while (timer.Elapsed < receiveTimeout && !this.cancellationTokenSource.IsCancellationRequested)
            {
                using (var txn = this.stateManager.CreateTransaction())
                {
                    string activityId;
                    if (this.inMemoryQueue.TryDequeue(out activityId))
                    {
                        var activity = await this.activityQueue.TryGetValueAsync(txn, activityId);
                        if (activity.HasValue)
                        {
                            return activity.Value;
                        }
                        throw new Exception("Internal server error");
                    }
                }
                await Task.Delay(100, this.cancellationTokenSource.Token);
                await PopulateInMemoryActivities();
            }
            return null;
        }

        public async Task CompleteWorkItem(ITransaction transaction, TaskActivityWorkItem workItem)
        {
            await this.activityQueue.TryRemoveAsync(transaction, workItem.Id);
            //Todo: Need to cleanup the lock table only after the transaction commit...
            //bool ignored;
            //this.lockTable.TryRemove(workItem.Id, out ignored);
        }

        public async Task AppendBatch(ITransaction transaction, IList<TaskMessage> messages)
        {
            if (messages.Count > 0)
            {
                //lock (syncLock)
                //{
                //    this.newMessagesInStore = true;
                //}
                foreach (var message in messages)
                {
                    var id = Guid.NewGuid().ToString();
                    await this.activityQueue.AddAsync(transaction, id, message);
                }
            }
        }

        async Task PopulateInMemoryActivities()
        {
            //var shouldFetchActivitiesInMemory = false;
            //lock (syncLock)
            //{
            //    if (this.newMessagesInStore)
            //    {
            //        this.newMessagesInStore = false;
            //        shouldFetchActivitiesInMemory = true;
            //    }
            //}

            //if (shouldFetchActivitiesInMemory)
            if (!this.cancellationTokenSource.IsCancellationRequested)
            {
                using (var txn = this.stateManager.CreateTransaction())
                {
                    var enumerable = await this.activityQueue.CreateEnumerableAsync(txn, EnumerationMode.Unordered);
                    using (var enumerator = enumerable.GetAsyncEnumerator())
                    {
                        while (await enumerator.MoveNextAsync(this.cancellationTokenSource.Token))
                        {
                            var activityId = enumerator.Current.Key;
                            if (!this.lockTable.ContainsKey(activityId))
                            {
                                this.inMemoryQueue.Enqueue(activityId);
                                this.lockTable.TryAdd(activityId, true);
                            }
                        }
                    }
                }
            }
        }
    }
}
