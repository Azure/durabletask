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
    using System.Diagnostics;
    using System.Diagnostics.Contracts;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Data;
    using Microsoft.ServiceFabric.Data.Collections;

    public class FabricOrchestrationService : IOrchestrationService
    {
        IReliableStateManager stateManager;

        const string OrchestrationDictionaryName = "Orchestrations";
        const string ActivitiesQueueName = "Activities";

        IReliableDictionary<string, SessionDocument> orchestrations;
        IReliableQueue<TaskMessage> activities;

        SessionDocument currentSession;
        TaskMessage currentActivity;

        public FabricOrchestrationService(IReliableStateManager stateManager)
        {
            if (stateManager == null)
            {
                throw new ArgumentNullException(nameof(stateManager));
            }

            this.stateManager = stateManager;
        }

        public async Task StartAsync()
        {
            var existingValue = await this.stateManager.TryGetAsync<IReliableDictionary<string, SessionDocument>>(OrchestrationDictionaryName);
            if (!existingValue.HasValue)
            {
                throw new Exception("Unexpected exception, by the time start is called, the reliable dictionary for sessions should have been there");
            }
            this.orchestrations = existingValue.Value;

            var existingActivityQueue = await this.stateManager.TryGetAsync<IReliableQueue<TaskMessage>>(ActivitiesQueueName);
            if (!existingActivityQueue.HasValue)
            {
                throw new Exception("Unexpected exception, by the time start is called, the reliable queue for activities should have been there");
            }
            this.activities = existingActivityQueue.Value;
        }

        public Task StopAsync()
        {
            throw new NotImplementedException();
        }

        public Task StopAsync(bool isForced)
        {
            throw new NotImplementedException();
        }

        public Task CreateAsync()
        {
            return CreateAsync(true);
        }

        public async Task CreateAsync(bool recreateInstanceStore)
        {
            //Todo: Do we need a transaction for this?
            await this.stateManager.RemoveAsync(OrchestrationDictionaryName);
            await this.stateManager.RemoveAsync(ActivitiesQueueName);

            this.orchestrations = await this.stateManager.GetOrAddAsync<IReliableDictionary<string, SessionDocument>>(OrchestrationDictionaryName);
            this.activities = await this.stateManager.GetOrAddAsync<IReliableQueue<TaskMessage>>(ActivitiesQueueName);
        }

        public async Task CreateIfNotExistsAsync()
        {
            var existingValue = await this.stateManager.TryGetAsync<IReliableDictionary<string, SessionDocument>>(OrchestrationDictionaryName);
            if (!existingValue.HasValue)
            {
                this.orchestrations = await this.stateManager.GetOrAddAsync<IReliableDictionary<string, SessionDocument>>(OrchestrationDictionaryName);
            }
            else
            {
                this.orchestrations = existingValue.Value;
            }

            var existingActivityQueue = await this.stateManager.TryGetAsync<IReliableQueue<TaskMessage>>(ActivitiesQueueName);
            if (!existingActivityQueue.HasValue)
            {
                this.activities = await this.stateManager.GetOrAddAsync<IReliableQueue<TaskMessage>>(ActivitiesQueueName);
            }
            else
            {
                this.activities = existingActivityQueue.Value;
            }
        }

        public Task DeleteAsync()
        {
            return DeleteAsync(true);
        }

        public async Task DeleteAsync(bool deleteInstanceStore)
        {
            await this.stateManager.RemoveAsync(OrchestrationDictionaryName);
            await this.stateManager.RemoveAsync(ActivitiesQueueName);
        }

        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            throw new NotImplementedException();
        }

        public int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            throw new NotImplementedException();
        }

        public int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            throw new NotImplementedException();
        }

        public int TaskOrchestrationDispatcherCount => 1;
        public int MaxConcurrentTaskOrchestrationWorkItems => 1;

        public async Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            Contract.Assert(this.currentSession == null, "How come we get a call for another session while a session is being processed?");

            this.currentSession = await this.AcceptSessionAsync(receiveTimeout, cancellationToken);

            if (this.currentSession == null)
            {
                return null;
            }

            return new TaskOrchestrationWorkItem()
            {
                NewMessages = this.currentSession.GetMessages(),
                InstanceId = this.currentSession.Id,
                OrchestrationRuntimeState = this.currentSession.SessionState
            };
        }

        async Task<SessionDocument> AcceptSessionAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            //Todo: This is O(N) worst case and we perhaps need to optimize this path
            Stopwatch timer = Stopwatch.StartNew();
            while (timer.Elapsed < receiveTimeout && !cancellationToken.IsCancellationRequested)
            {
                using (var tx = this.stateManager.CreateTransaction())
                {
                    var enumerable = await this.orchestrations.CreateEnumerableAsync(tx, EnumerationMode.Unordered);
                    using (var enumerator = enumerable.GetAsyncEnumerator())
                    {
                        while (await enumerator.MoveNextAsync(cancellationToken))
                        {
                            var entry = enumerator.Current;

                            if (!entry.Value.AnyActiveMessages())
                            {
                                continue;
                            }

                            return entry.Value;
                        }
                    }
                }
                await Task.Delay(100, cancellationToken);
            }

            return null;
        }

        public Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public async Task CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState,
            IList<TaskMessage> outboundMessages,
            IList<TaskMessage> orchestratorMessages,
            IList<TaskMessage> timerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState orchestrationState)
        {
            Contract.Assert(string.Equals(currentSession.Id, workItem.InstanceId), "Unexpected thing happened, complete should be called with the same session as locked");

            if (timerMessages != null && timerMessages.Count > 0)
            {
                throw new ArgumentException("timer messages not supported yet");
            }

            if (orchestratorMessages != null && orchestratorMessages.Count > 0)
            {
                throw new ArgumentException("Sub orchestrations are not supported yet");
            }

            if (continuedAsNewMessage != null)
            {
                throw new ArgumentException("Continued as new is not supported yet");
            }

            using (var txn = this.stateManager.CreateTransaction())
            {
                // Todo: This will be ideally using immutable semantics
                this.currentSession.SessionState = newOrchestrationRuntimeState;
                this.currentSession.CompleteLockedMessages();

                foreach (var workerMessage in outboundMessages)
                {
                    await this.activities.EnqueueAsync(txn, workerMessage);
                }

                await this.orchestrations.SetAsync(txn, workItem.InstanceId, this.currentSession);

                await txn.CommitAsync();
                this.currentSession = null;
            }
        }

        public Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            this.currentSession = null;
            return Task.FromResult<object>(null);
        }

        public Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            //Todo: When is a good time to take the session out from dictionary altogether?
            return Task.FromResult<object>(null);
        }

        public int TaskActivityDispatcherCount => 1;
        public int MaxConcurrentTaskActivityWorkItems => 1;

        public async Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            Contract.Assert(this.currentActivity == null, "How come we get a call for another activity while an activity is running?");

            this.currentActivity = await this.GetNextWorkItem(receiveTimeout, cancellationToken);

            if (this.currentActivity != null)
            {
                return new TaskActivityWorkItem()
                {
                    Id = "N/A", //Todo: Need to persist the guid??
                    TaskMessage = this.currentActivity
                };
            }

            return null;
        }

        async Task<TaskMessage> GetNextWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            Stopwatch timer = Stopwatch.StartNew();
            while (timer.Elapsed < receiveTimeout && !cancellationToken.IsCancellationRequested)
            {
                using (var txn = this.stateManager.CreateTransaction())
                {
                    var activityValue = await this.activities.TryPeekAsync(txn, LockMode.Default);
                    if (activityValue.HasValue)
                    {
                        return activityValue.Value;
                    }
                }
                await Task.Delay(100, cancellationToken);
            }
            return null;
        }

        public async Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
        {
            Contract.Assert(workItem.TaskMessage == this.currentActivity, "Unexpected thing happened, complete called for an activity that's not the current activity");

            using (var txn = this.stateManager.CreateTransaction())
            {
                await this.activities.TryDequeueAsync(txn);
                var sessionId = workItem.TaskMessage.OrchestrationInstance.InstanceId;
                var orchestrationValue = await this.orchestrations.TryGetValueAsync(txn, sessionId, LockMode.Update);
                if (!orchestrationValue.HasValue)
                {
                    throw new Exception("Unexpected exception : Trying to complete a work item and could not find the corresponding orchestration");
                }
                var session = orchestrationValue.Value;
                session.AppendMessage(responseMessage);
                await this.orchestrations.SetAsync(txn, sessionId, session);

                await txn.CommitAsync();
                this.currentActivity = null;
            }
        }

        public Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            this.currentActivity = null;
            return Task.FromResult(workItem);
        }

        public Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            Contract.Assert(workItem.TaskMessage == this.currentActivity, "Unexpected thing happened, renew lock called for an activity that's not the current activity");
            return Task.FromResult(workItem);
        }
    }
}
