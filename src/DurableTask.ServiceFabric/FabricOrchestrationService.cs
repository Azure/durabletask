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
        ActivitiesProvider activitiesProvider;

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
        }

        public async Task StartAsync()
        {
            var activities = await this.stateManager.GetOrAddAsync<IReliableQueue<TaskMessage>>(Constants.ActivitiesQueueName);
            var orchestrations = await this.stateManager.GetOrAddAsync<IReliableDictionary<string, PersistentSession>>(Constants.OrchestrationDictionaryName);
            this.activitiesProvider = new ActivitiesProvider(this.stateManager, activities);
            this.orchestrationProvider = new SessionsProvider(stateManager, orchestrations);
            await this.orchestrationProvider.StartAsync();
        }

        public Task StopAsync()
        {
            return StopAsync(false);
        }

        public Task StopAsync(bool isForced)
        {
            this.orchestrationProvider.Stop();
            return Task.FromResult<object>(null);
        }

        public Task CreateAsync()
        {
            return CreateAsync(true);
        }

        public async Task CreateAsync(bool recreateInstanceStore)
        {
            await DeleteAsync(deleteInstanceStore: recreateInstanceStore);
            // Actual creation will be done on demand when we call GetOrAddAsync in StartAsync method.
        }

        public Task CreateIfNotExistsAsync()
        {
            return Task.FromResult<object>(null);
        }

        public Task DeleteAsync()
        {
            return DeleteAsync(true);
        }

        public async Task DeleteAsync(bool deleteInstanceStore)
        {
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

            if (continuedAsNewMessage != null)
            {
                throw new Exception("ContinueAsNew is not supported yet");
            }

            using (var txn = this.stateManager.CreateTransaction())
            {
                await this.activitiesProvider.AppendBatch(txn, outboundMessages);

                await this.orchestrationProvider.CompleteAndUpdateSession(txn, this.currentSession, newOrchestrationRuntimeState, timerMessages);

                if (orchestratorMessages?.Count > 0)
                {
                    await this.orchestrationProvider.AppendMessageBatchAsync(txn, orchestratorMessages);
                }

                // Something more is needed for ContinuedAsNew support...
                //if (continuedAsNewMessage != null)
                //{
                //    await this.orchestrationProvider.AppendMessageAsync(txn, continuedAsNewMessage);
                //}

                // Todo: This is not yet part of the transaction
                if (this.instanceStore != null)
                {
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

            this.currentActivity = await this.activitiesProvider.GetNextWorkItem(receiveTimeout, cancellationToken);

            if (this.currentActivity != null)
            {
                return new TaskActivityWorkItem()
                {
                    Id = Guid.NewGuid().ToString(), //Todo: Do we need to persist this in activity queue?
                    TaskMessage = this.currentActivity
                };
            }

            return null;
        }

        public async Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
        {
            Contract.Assert(workItem.TaskMessage == this.currentActivity, "Unexpected thing happened, complete called for an activity that's not the current activity");

            using (var txn = this.stateManager.CreateTransaction())
            {
                await this.activitiesProvider.CompleteWorkItem(txn, workItem.TaskMessage);
                var sessionId = workItem.TaskMessage.OrchestrationInstance.InstanceId;
                await this.orchestrationProvider.AppendMessageAsync(txn, responseMessage);
                await txn.CommitAsync();
                this.currentActivity = null;
            }
        }

        public Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            Contract.Assert(workItem.TaskMessage == this.currentActivity, "Unexpected thing happened, abandon called for an activity that's not the current activity");
            this.currentActivity = null;
            return Task.FromResult(workItem);
        }

        public bool ProcessWorkItemSynchronously => true;

        public Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            Contract.Assert(workItem.TaskMessage == this.currentActivity, "Unexpected thing happened, renew lock called for an activity that's not the current activity");
            return Task.FromResult(workItem);
        }
    }
}
