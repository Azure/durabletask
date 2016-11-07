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
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Tracking;
    using Microsoft.ServiceFabric.Data;

    class FabricOrchestrationService : IOrchestrationService
    {
        readonly IReliableStateManager stateManager;
        readonly IFabricOrchestrationServiceInstanceStore instanceStore;
        readonly SessionsProvider orchestrationProvider;
        readonly ActivityProvider<string, TaskMessage> activitiesProvider;
        readonly ScheduledMessageProvider scheduledMessagesProvider;
        readonly FabricOrchestrationProviderSettings settings;

        public FabricOrchestrationService(IReliableStateManager stateManager,
            SessionsProvider orchestrationProvider,
            IFabricOrchestrationServiceInstanceStore instanceStore,
            FabricOrchestrationProviderSettings settings)
        {
            if (stateManager == null)
            {
                throw new ArgumentNullException(nameof(stateManager));
            }

            this.stateManager = stateManager;
            this.orchestrationProvider = orchestrationProvider;
            this.instanceStore = instanceStore;
            this.settings = settings;
            this.activitiesProvider = new ActivityProvider<string, TaskMessage>(this.stateManager, Constants.ActivitiesQueueName);
            this.scheduledMessagesProvider = new ScheduledMessageProvider(this.stateManager, Constants.ScheduledMessagesDictionaryName, orchestrationProvider);
        }

        public async Task StartAsync()
        {
            await this.activitiesProvider.StartAsync();
            await this.scheduledMessagesProvider.StartAsync();
            await this.instanceStore.StartAsync();
            await this.orchestrationProvider.StartAsync();
        }

        public Task StopAsync()
        {
            return StopAsync(false);
        }

        public async Task StopAsync(bool isForced)
        {
            await this.orchestrationProvider.StopAsync();
            await this.instanceStore.StopAsync(isForced);
            await this.scheduledMessagesProvider.StopAsync();
            await this.activitiesProvider.StopAsync();
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
            await this.stateManager.RemoveAsync(Constants.ScheduledMessagesDictionaryName);
            await this.stateManager.RemoveAsync(Constants.ActivitiesQueueName);

            if (deleteInstanceStore)
            {
                await this.stateManager.RemoveAsync(Constants.InstanceStoreDictionaryName);
            }
        }

        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            return false;
        }

        public int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            ProviderEventSource.Instance.LogException(exception.Message, exception.StackTrace);
            //Todo: Need to fine tune
            if (exception is TimeoutException)
            {
                return 1;
            }

            return 0;
        }

        public int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            ProviderEventSource.Instance.LogException(exception.Message, exception.StackTrace);
            //Todo: Need to fine tune
            if (exception is TimeoutException)
            {
                return 1;
            }

            return 0;
        }

        public int TaskOrchestrationDispatcherCount => this.settings.TaskOrchestrationDispatcherSettings.DispatcherCount;
        public int MaxConcurrentTaskOrchestrationWorkItems => this.settings.TaskOrchestrationDispatcherSettings.MaxConcurrentOrchestrations;

        // Note: Do not rely on cancellationToken parameter to this method because the top layer does not yet implement any cancellation.
        public async Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            var currentSession = await this.orchestrationProvider.AcceptSessionAsync(receiveTimeout);

            if (currentSession == null)
            {
                return null;
            }

            var newMessages = this.orchestrationProvider.GetSessionMessages(currentSession);
            return new TaskOrchestrationWorkItem()
            {
                NewMessages = newMessages,
                InstanceId = currentSession.SessionId,
                OrchestrationRuntimeState = new OrchestrationRuntimeState(currentSession.SessionState)
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
            if (continuedAsNewMessage != null)
            {
                throw new Exception("ContinueAsNew is not supported yet");
            }

            using (var txn = this.stateManager.CreateTransaction())
            {
                var previousState = workItem.OrchestrationRuntimeState;
                IList<string> sessionsToEnqueue = null;
                List<Message<string, TaskMessage>> scheduledMessages = null;
                List<Message<string, TaskMessage>> activityMessages = null;

                var newSessionValue = await this.orchestrationProvider.CompleteAndUpdateSession(txn, workItem.InstanceId, newOrchestrationRuntimeState);

                if (outboundMessages?.Count > 0)
                {
                    activityMessages = outboundMessages.Select(m => new Message<string, TaskMessage>(Guid.NewGuid().ToString(), m)).ToList();
                    await this.activitiesProvider.SendBatchBeginAsync(txn, activityMessages);
                }

                if (timerMessages?.Count > 0)
                {
                    scheduledMessages = timerMessages.Select(m => new Message<string, TaskMessage>(Guid.NewGuid().ToString(), m)).ToList();
                    await this.scheduledMessagesProvider.SendBatchBeginAsync(txn, scheduledMessages);
                }

                if (orchestratorMessages?.Count > 0)
                {
                    if (previousState?.ParentInstance != null)
                    {
                        sessionsToEnqueue = await this.orchestrationProvider.TryAppendMessageBatchAsync(txn, orchestratorMessages);
                    }
                    else
                    {
                        await this.orchestrationProvider.AppendMessageBatchAsync(txn, orchestratorMessages);
                        sessionsToEnqueue = orchestratorMessages.Select(m => m.OrchestrationInstance.InstanceId).ToList();
                    }
                }

                if (this.instanceStore != null)
                {
                    await this.instanceStore.WriteEntitesAsync(txn, new InstanceEntityBase[]
                    {
                        new OrchestrationStateInstanceEntity()
                        {
                            State = Utils.BuildOrchestrationState(workItem.OrchestrationRuntimeState)
                        }
                    });
                }

                await txn.CommitAsync();

                this.orchestrationProvider.TryUnlockSession(workItem.InstanceId, putBackInQueue: newSessionValue.Messages.Any());
                if (activityMessages != null)
                {
                    this.activitiesProvider.SendBatchComplete(activityMessages);
                }
                if (scheduledMessages != null)
                {
                    this.scheduledMessagesProvider.SendBatchComplete(scheduledMessages);
                }
                if (sessionsToEnqueue != null)
                {
                    foreach (var sessionId in sessionsToEnqueue)
                    {
                        this.orchestrationProvider.TryEnqueueSession(sessionId);
                    }
                }
            }
        }

        public Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            this.orchestrationProvider.TryUnlockSession(workItem.InstanceId, putBackInQueue: true);
            return Task.FromResult<object>(null);
        }

        public async Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            if (workItem.OrchestrationRuntimeState.OrchestrationStatus.IsTerminalState())
            {
                using (var txn = this.stateManager.CreateTransaction())
                {
                    await this.orchestrationProvider.DropSession(txn, workItem.InstanceId);
                    await txn.CommitAsync();
                    ProviderEventSource.Instance.LogOrchestrationFinished(workItem.InstanceId,
                        workItem.OrchestrationRuntimeState.OrchestrationStatus.ToString(),
                        (workItem.OrchestrationRuntimeState.CompletedTime - workItem.OrchestrationRuntimeState.CreatedTime).TotalSeconds,
                        workItem.OrchestrationRuntimeState.Output);
                }
            }
        }

        public int TaskActivityDispatcherCount => this.settings.TaskActivityDispatcherSettings.DispatcherCount;
        public int MaxConcurrentTaskActivityWorkItems => this.settings.TaskActivityDispatcherSettings.MaxConcurrentActivities;

        // Note: Do not rely on cancellationToken parameter to this method because the top layer does not yet implement any cancellation.
        public async Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            var message = await this.activitiesProvider.ReceiveAsync(receiveTimeout);

            if (message != null)
            {
                return new TaskActivityWorkItem()
                {
                    Id = message.Key,
                    TaskMessage = message.Value
                };
            }

            return null;
        }

        public async Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
        {
            using (var txn = this.stateManager.CreateTransaction())
            {
                await this.activitiesProvider.CompleteAsync(txn, workItem.Id);
                bool added = await this.orchestrationProvider.TryAppendMessageAsync(txn, responseMessage);
                await txn.CommitAsync();
                if (added)
                {
                    this.orchestrationProvider.TryEnqueueSession(responseMessage.OrchestrationInstance.InstanceId);
                }
            }
        }

        public Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            return this.activitiesProvider.AandonAsync(workItem.Id);
        }

        public Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            return Task.FromResult(workItem);
        }
    }
}
