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

using DurableTask.Common;
using DurableTask.Tracking;

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
        IOrchestrationServiceInstanceStore instanceStore;

        SessionsProvider orchestrationProvider;
        IReliableQueue<TaskMessage> activities;

        PersistentSession currentSession;
        TaskMessage currentActivity;

        public FabricOrchestrationService(IReliableStateManager stateManager, IOrchestrationServiceInstanceStore instanceStore)
        {
            if (stateManager == null)
            {
                throw new ArgumentNullException(nameof(stateManager));
            }

            this.stateManager = stateManager;
            this.instanceStore = instanceStore;
            orchestrationProvider = new SessionsProvider(stateManager);
        }

        public Task StartAsync()
        {
            return Task.FromResult<object>(null);
        }

        public Task StopAsync()
        {
            return StopAsync(false);
        }

        public Task StopAsync(bool isForced)
        {
            return Task.FromResult<object>(null);
        }

        public Task CreateAsync()
        {
            return CreateAsync(true);
        }

        public async Task CreateAsync(bool recreateInstanceStore)
        {
            await DeleteAsync(deleteInstanceStore: recreateInstanceStore);
        }

        public Task CreateIfNotExistsAsync()
        {
            // This is not really needed because of winfab API which allows us to GetOrAdd
            return Task.FromResult<object>(null);
        }

        public Task DeleteAsync()
        {
            return DeleteAsync(true);
        }

        public async Task DeleteAsync(bool deleteInstanceStore)
        {
            //Todo: Do we need a transaction for this?
            await this.stateManager.RemoveAsync(Constants.OrchestrationDictionaryName);
            await this.stateManager.RemoveAsync(Constants.ActivitiesQueueName);
        }

        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            //Todo: Do we need to enforce a limit here?
            return false;
        }

        public int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            //Todo: Need to fine tune
            if (exception is TimeoutException)
            {
                return 1;
            }

            return 0;
        }

        public int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            //Todo: Need to fine tune
            if (exception is TimeoutException)
            {
                return 1;
            }

            return 0;
        }

        public int TaskOrchestrationDispatcherCount => 1;
        public int MaxConcurrentTaskOrchestrationWorkItems => 1;

        public async Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            Contract.Assert(this.currentSession == null, "How come we get a call for another session while a session is being processed?");

            this.currentSession = await this.orchestrationProvider.AcceptSessionAsync(receiveTimeout, cancellationToken);

            if (this.currentSession == null)
            {
                return null;
            }

            var newMessages = this.orchestrationProvider.GetSessionMessages(this.currentSession);
            return new TaskOrchestrationWorkItem()
            {
                NewMessages = newMessages,
                InstanceId = this.currentSession.SessionId,
                OrchestrationRuntimeState = this.currentSession.SessionState
            };
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
            Contract.Assert(this.currentSession != null && string.Equals(currentSession.SessionId, workItem.InstanceId), "Unexpected thing happened, complete should be called with the same session as locked");

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
                foreach (var workerMessage in outboundMessages)
                {
                    await this.activities.EnqueueAsync(txn, workerMessage);
                }

                await this.orchestrationProvider.CompleteSessionMessagesAsync(txn, this.currentSession, newOrchestrationRuntimeState);

                if (this.instanceStore != null)
                {
                    // Todo: This is not yet part of the transaction
                    await this.instanceStore.WriteEntitesAsync(new InstanceEntityBase[]
                    {
                        new OrchestrationStateInstanceEntity()
                        {
                            State = Utils.BuildOrchestrationState(workItem.OrchestrationRuntimeState)
                        }
                    });
                }

                await txn.CommitAsync();
                this.currentSession = null;
            }
        }

        public Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            Contract.Assert(this.currentSession != null && string.Equals(this.currentSession.SessionId, workItem.InstanceId), "Unexpected thing happened, abandon should be called with the same session as locked");
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
                await this.orchestrationProvider.AppendMessageAsync(txn, sessionId, responseMessage);
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
