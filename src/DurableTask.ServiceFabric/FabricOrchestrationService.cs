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
    using System.Fabric;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.History;
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
            this.stateManager = stateManager ?? throw new ArgumentNullException(nameof(stateManager));
            this.orchestrationProvider = orchestrationProvider;
            this.instanceStore = instanceStore;
            this.settings = settings;
            this.activitiesProvider = new ActivityProvider<string, TaskMessage>(this.stateManager, Constants.ActivitiesQueueName);
            this.scheduledMessagesProvider = new ScheduledMessageProvider(this.stateManager, Constants.ScheduledMessagesDictionaryName, orchestrationProvider);
        }

        public Task StartAsync()
        {
            return Task.WhenAll(this.activitiesProvider.StartAsync(),
                this.scheduledMessagesProvider.StartAsync(),
                this.instanceStore.StartAsync(),
                this.orchestrationProvider.StartAsync());
        }

        public Task StopAsync()
        {
            return StopAsync(false);
        }

        public Task StopAsync(bool isForced)
        {
            return Task.WhenAll(this.orchestrationProvider.StopAsync(),
                this.instanceStore.StopAsync(isForced),
                this.scheduledMessagesProvider.StopAsync(),
                this.activitiesProvider.StopAsync());
        }

        public Task CreateAsync()
        {
            return CreateAsync(true);
        }

        public Task CreateAsync(bool recreateInstanceStore)
        {
            return DeleteAsync(deleteInstanceStore: recreateInstanceStore);
            // Actual creation will be done on demand when we call GetOrAddAsync in StartAsync method.
        }

        public Task CreateIfNotExistsAsync()
        {
            return CompletedTask.Default;
        }

        public Task DeleteAsync()
        {
            return DeleteAsync(true);
        }

        public Task DeleteAsync(bool deleteInstanceStore)
        {
            List<Task> tasks = new List<Task>();
            tasks.Add(this.stateManager.RemoveAsync(Constants.OrchestrationDictionaryName));
            tasks.Add(this.stateManager.RemoveAsync(Constants.ScheduledMessagesDictionaryName));
            tasks.Add(this.stateManager.RemoveAsync(Constants.ActivitiesQueueName));

            if (deleteInstanceStore)
            {
                tasks.Add(this.stateManager.RemoveAsync(Constants.InstanceStoreDictionaryName));
            }

            return Task.WhenAll(tasks);
        }

        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            return false;
        }

        public int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            //Todo: Need to fine tune
            if (exception is TimeoutException)
            {
                return 2;
            }

            return 0;
        }

        public int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            //Todo: Need to fine tune
            if (exception is TimeoutException)
            {
                return 2;
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

            List<TaskMessage> newMessages;
            try
            {
                newMessages = await this.orchestrationProvider.ReceiveSessionMessagesAsync(currentSession);
            }
            catch(Exception e)
            {
                ProviderEventSource.Log.ExceptionWhileProcessingReliableCollectionTransaction($"OrchestrationId = '{currentSession.SessionId}', Action = 'ReceiveSessionMessagesAsync'", e.ToString());
                this.orchestrationProvider.TryUnlockSession(currentSession.SessionId);
                throw;
            }

            if (newMessages.Count == 0)
            {
                this.orchestrationProvider.TryUnlockSession(currentSession.SessionId);
                return null;
            }

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

            IList<string> sessionsToEnqueue = null;
            List<Message<string, TaskMessage>> scheduledMessages = null;
            List<Message<string, TaskMessage>> activityMessages = null;

            await RetryHelper.ExecuteWithRetryOnTransient(async () =>
            {
                bool retryOnException;
                do
                {
                    try
                    {
                        retryOnException = false;
                        sessionsToEnqueue = null;
                        scheduledMessages = null;
                        activityMessages = null;

                        using (var txn = this.stateManager.CreateTransaction())
                        {
                            await this.orchestrationProvider.CompleteAndUpdateSession(txn, workItem.InstanceId, newOrchestrationRuntimeState);

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
                                if (workItem.OrchestrationRuntimeState?.ParentInstance != null)
                                {
                                    sessionsToEnqueue = await this.orchestrationProvider.TryAppendMessageBatchAsync(txn, orchestratorMessages);
                                }
                                else
                                {
                                    await this.orchestrationProvider.AppendMessageBatchAsync(txn, orchestratorMessages);
                                    sessionsToEnqueue = orchestratorMessages.Select(m => m.OrchestrationInstance.InstanceId).ToList();
                                }
                            }

                            if (this.instanceStore != null && orchestrationState != null)
                            {
                                await this.instanceStore.WriteEntitesAsync(txn, new InstanceEntityBase[]
                                {
                            new OrchestrationStateInstanceEntity()
                            {
                                State = orchestrationState
                            }
                                });
                            }

                            await txn.CommitAsync();
                        }
                    }
                    catch (FabricReplicationOperationTooLargeException ex)
                    {
                        retryOnException = true;
                        newOrchestrationRuntimeState = null;
                        outboundMessages = null;
                        timerMessages = null;
                        orchestratorMessages = null;
                        if (orchestrationState != null)
                        {
                            orchestrationState.OrchestrationStatus = OrchestrationStatus.Failed;
                            orchestrationState.Output = $"Fabric exception when trying to process orchestration: {ex}. Investigate and consider reducing the serialization size of orchestration inputs/outputs/overall length to avoid the issue.";
                        }
                    }
                } while (retryOnException);
            }, uniqueActionIdentifier: $"OrchestrationId = '{workItem.InstanceId}', Action = '{nameof(CompleteTaskOrchestrationWorkItemAsync)}'");

            this.orchestrationProvider.TryUnlockSession(workItem.InstanceId);
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

        public Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            this.orchestrationProvider.TryUnlockSession(workItem.InstanceId, abandon: true);
            return CompletedTask.Default;
        }

        public async Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            if (workItem.OrchestrationRuntimeState.OrchestrationStatus.IsTerminalState())
            {
                await this.orchestrationProvider.DropSession(workItem.InstanceId);
                ProviderEventSource.Log.OrchestrationFinished(workItem.InstanceId,
                    workItem.OrchestrationRuntimeState.OrchestrationStatus.ToString(),
                    (workItem.OrchestrationRuntimeState.CompletedTime - workItem.OrchestrationRuntimeState.CreatedTime).TotalSeconds,
                    workItem.OrchestrationRuntimeState.Output);
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
            bool added = false;

            await RetryHelper.ExecuteWithRetryOnTransient(async () =>
            {
                bool retryOnException;
                do
                {
                    try
                    {
                        added = false;
                        retryOnException = false;

                        using (var txn = this.stateManager.CreateTransaction())
                        {
                            await this.activitiesProvider.CompleteAsync(txn, workItem.Id);
                            added = await this.orchestrationProvider.TryAppendMessageAsync(txn, responseMessage);
                            await txn.CommitAsync();
                        }
                    }
                    catch (FabricReplicationOperationTooLargeException ex)
                    {
                        retryOnException = true;
                        var originalEvent = responseMessage.Event;
                        int taskScheduledId = GetTaskScheduledId(originalEvent);
                        string details = $"Fabric exception when trying to save activity result: {ex}. Consider reducing the serialization size of activity result to avoid the issue.";
                        responseMessage.Event = new TaskFailedEvent(originalEvent.EventId, taskScheduledId, ex.Message, details);
                    }
                } while (retryOnException);
            }, uniqueActionIdentifier: $"OrchestrationId = '{responseMessage.OrchestrationInstance.InstanceId}', ActivityId = '{workItem.Id}', Action = '{nameof(CompleteTaskActivityWorkItemAsync)}'");

            if (added)
            {
                this.orchestrationProvider.TryEnqueueSession(responseMessage.OrchestrationInstance.InstanceId);
            }
        }

        public Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            this.activitiesProvider.Abandon(workItem.Id);
            return CompletedTask.Default;
        }

        public Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            return Task.FromResult(workItem);
        }

        int GetTaskScheduledId(HistoryEvent historyEvent)
        {
            TaskCompletedEvent tce = historyEvent as TaskCompletedEvent;
            if (tce != null)
            {
                return tce.TaskScheduledId;
            }

            TaskFailedEvent tfe = historyEvent as TaskFailedEvent;
            if (tfe != null)
            {
                return tfe.TaskScheduledId;
            }

            return -1;
        }
    }
}
