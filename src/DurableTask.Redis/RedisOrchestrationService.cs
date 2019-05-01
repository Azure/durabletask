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

namespace DurableTask.Redis
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using StackExchange.Redis;

    /// <summary>
    /// Orchestration service provider for the Durable Task Framework which uses Redis as the durable store.
    /// </summary>
    public class RedisOrchestrationService : IOrchestrationService, IOrchestrationServiceClient
    {
        private readonly SemaphoreSlim startLock = new SemaphoreSlim(1);

        private readonly RedisOrchestrationServiceSettings settings;

        // Initialized in StartAsync()
        private ConnectionMultiplexer redisConnection;
        private OrchestrationSessionPartitionHandler partitionOrchestrationManager;
        private ActivityTaskHandler activityTaskManager;
        private WorkerRecycler workerRecycler;
        private RedisLogger logger;
        private string workerGuid;

        /// <summary>
        /// Initializes a new instance of the <see cref="RedisOrchestrationService"/> class.
        /// </summary>
        /// <param name="settings">The settings used to configure the orchestration service.</param>
        public RedisOrchestrationService(RedisOrchestrationServiceSettings settings)
        {
            this.settings = RedisOrchestrationServiceSettings.Validate(settings);
        }

        /// <inheritdoc />
        public int TaskOrchestrationDispatcherCount => 1;

        /// <inheritdoc />
        public int MaxConcurrentTaskOrchestrationWorkItems => this.settings.MaxConcurrentTaskOrchestrationWorkItems;

        /// <inheritdoc />
        public BehaviorOnContinueAsNew EventBehaviourForContinueAsNew => BehaviorOnContinueAsNew.Carryover;

        /// <inheritdoc />
        public int TaskActivityDispatcherCount => 1;

        /// <inheritdoc />
        public int MaxConcurrentTaskActivityWorkItems => this.settings.MaxConcurrentTaskActivityWorkItems;


        #region Orchestration Methods
        /// <inheritdoc />
        public async Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            if (this.partitionOrchestrationManager == null)
            {
                await StartAsync();
            }
            return await this.partitionOrchestrationManager.GetSingleOrchestrationSessionAsync(receiveTimeout, cancellationToken);
        }

        /// <inheritdoc />
        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            return currentMessageCount > MaxConcurrentTaskOrchestrationWorkItems;
        }

        /// <inheritdoc />
        public async Task CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState,
            IList<TaskMessage> outboundMessages,
            IList<TaskMessage> orchestratorMessages,
            IList<TaskMessage> timerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState orchestrationState)
        {
            if (this.partitionOrchestrationManager == null || this.redisConnection == null)
            {
                await StartAsync();
            }
            string orchestrationId = workItem.InstanceId;
            RedisTransactionBuilder transaction = this.partitionOrchestrationManager.CreateExistingOrchestrationTransaction(orchestrationId);

            List<string> events = newOrchestrationRuntimeState.Events.Select(histEvent => histEvent as TaskCompletedEvent)
                .Where(taskCompletedEvent => taskCompletedEvent != null)
                .OrderBy(task => task.TaskScheduledId)
                .Select(TaskCompletedEvent => $"{{\"id\": {TaskCompletedEvent.TaskScheduledId}, \"Result\": {TaskCompletedEvent.Result}}}")
                .ToList();
            string logMessage = "Current events processed: " + string.Join(",", events);
            await this.logger.LogAsync(logMessage);

            transaction.SetOrchestrationRuntimeState(orchestrationId, newOrchestrationRuntimeState);

            foreach (TaskMessage outboundMessage in outboundMessages)
            {
                transaction.SendActivityMessage(outboundMessage);
            }

            foreach(TaskMessage message in orchestratorMessages)
            {
                transaction.SendControlQueueMessage(message);
            }

            if (continuedAsNewMessage != null)
            {
                transaction.SendControlQueueMessage(continuedAsNewMessage);
            } 

            // TODO send timer messages in transaction

            transaction.AddOrchestrationStateToHistory(orchestrationId: orchestrationId,
                state: orchestrationState);

            transaction.RemoveItemsFromOrchestrationQueue(orchestrationId, workItem.NewMessages.Count);

            bool transactionSucceeded = await transaction.CommitTransactionAsync();

            if (transactionSucceeded)
            {
                logMessage = $"Succeeded in transaction of finishing task orchestration item: execution id={workItem.OrchestrationRuntimeState.OrchestrationInstance.ExecutionId}, numMessagesProcessed: {workItem.NewMessages.Count}, numOutBoundMessages: {outboundMessages.Count}, numOrchMessages: {orchestratorMessages.Count}";
            }
            else
            {
                logMessage = $"Failed in transaction of finishing task orchestration item: execution id={workItem.OrchestrationRuntimeState.OrchestrationInstance.ExecutionId}, numMessagesProcessed: {workItem.NewMessages.Count}, numOutBoundMessages: {outboundMessages.Count}, numOrchMessages: {orchestratorMessages.Count}";
            }
            await this.logger.LogAsync(logMessage);
        }

        /// <inheritdoc />
        public Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            // No need to renew, as current lock implementation lasts as long as process holds it.
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public async Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            if (this.partitionOrchestrationManager == null || this.redisConnection == null)
            {
                await StartAsync();
            }
            await this.logger.LogAsync($"Releasing lock on orchestration {workItem.InstanceId}");
            this.partitionOrchestrationManager.ReleaseOrchestration(workItem.InstanceId);
        }

        /// <inheritdoc />
        public async Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            if (this.partitionOrchestrationManager == null || this.redisConnection == null)
            {
                await StartAsync();
            }
            // TODO: Handle differently then just releasing?
            await ReleaseTaskOrchestrationWorkItemAsync(workItem);
        }
        #endregion

        #region Activity Methods
        /// <inheritdoc />
        public async Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            if (this.activityTaskManager == null)
            {
                await StartAsync();
            }
            return await this.activityTaskManager.GetSingleTaskItem(receiveTimeout, cancellationToken);
        }

        /// <inheritdoc />
        public async Task CompleteTaskActivityWorkItemAsync(
            TaskActivityWorkItem workItem,
            TaskMessage responseMessage)
        {
            if (this.activityTaskManager == null || this.partitionOrchestrationManager == null)
            {
                await StartAsync();
            }
            RedisTransactionBuilder transactionBuilder = this.partitionOrchestrationManager.CreateExistingOrchestrationTransaction(responseMessage.OrchestrationInstance.InstanceId)
                .SendControlQueueMessage(responseMessage)
                .RemoveTaskMessageFromActivityQueue(workItem.TaskMessage, this.workerGuid);

            bool transactionSucceeded = await transactionBuilder.CommitTransactionAsync();

            string logMessage;
            if (transactionSucceeded)
            {
                logMessage = $"Succeeded in transaction of finishing task activity item: activity event id={workItem.TaskMessage.Event.EventId}, orchestration event id: {responseMessage.Event.EventId}";
            }
            else
            {
                logMessage = $"Failed in transaction of finishing task activity item: activity event id={workItem.TaskMessage.Event.EventId}, orchestration event id: {responseMessage.Event.EventId}";
            }
            await this.logger.LogAsync(logMessage);
        }

        /// <inheritdoc />
        public Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            // TODO: At this time this is sufficient, as locks last indefinitely
            return Task.FromResult(workItem);
        }

        /// <inheritdoc />
        public async Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            if (this.activityTaskManager == null)
            {
                await StartAsync();
            }
            IDatabase redisDb = this.redisConnection.GetDatabase();
            string activityProcessingQueueKey = RedisKeyNameResolver.GetTaskActivityProcessingQueueKey(this.settings.TaskHubName, this.workerGuid);
            string messageJson = RedisSerializer.SerializeObject(workItem.TaskMessage);
            await redisDb.ListRemoveAsync(activityProcessingQueueKey, messageJson);
        }
        #endregion

        #region Task Hub Methods
        /// <inheritdoc />
        public Task CreateAsync()
        {
            // No operation needed, Redis objects are created on the fly if necessary.
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task CreateAsync(bool recreateInstanceStore)
        {
            // No operation needed, Redis objects are created on the fly if necessary.
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task CreateIfNotExistsAsync()
        {
            // No operation needed, Redis objects are created on the fly if necessary.
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public async Task DeleteAsync()
        {
            if (this.redisConnection == null || !this.redisConnection.IsConnected)
            {
                this.redisConnection = await ConnectionMultiplexer.ConnectAsync(this.settings.RedisConnectionString);
            }
            IDatabase db = this.redisConnection.GetDatabase();
            EndPoint[] endpoints = this.redisConnection.GetEndPoints();
            IServer keyServer;
            if (endpoints.Length == 1)
            {
                // Grab the keys from the only server
                keyServer = this.redisConnection.GetServer(endpoints[0]);
            }
            else
            {
                // Grab the keys from a slave endpoint to avoid hitting the master too hard with the
                // expensive request. If no slaves are available, just grab the first available server
                IServer[] servers = endpoints.Select(ep => this.redisConnection.GetServer(ep)).ToArray();
                keyServer = servers.FirstOrDefault(server => server.IsSlave && server.IsConnected)
                    ?? servers.FirstOrDefault(server => server.IsConnected)
                    ?? throw new InvalidOperationException("None of the Redis servers are connected.");
            }
            // Grab all keys that have the task hub prefix
            RedisKey[] keysToDelete = keyServer.Keys(pattern: $"{this.settings.TaskHubName}.*").ToArray();
            await db.KeyDeleteAsync(keysToDelete);
        }

        /// <inheritdoc />
        public async Task DeleteAsync(bool deleteInstanceStore)
        {
            await this.DeleteAsync();
        }

        /// <inheritdoc />
        public async Task StartAsync()
        {
            await this.startLock.WaitAsync();

            if (this.workerGuid == null)
            {
                this.redisConnection = await ConnectionMultiplexer.ConnectAsync(this.settings.RedisConnectionString);
                this.workerRecycler = new WorkerRecycler(this.settings.TaskHubName, this.redisConnection);
                await this.workerRecycler.CleanupWorkersAsync();
                this.workerGuid = Guid.NewGuid().ToString();
                RegisterWorker();
                this.partitionOrchestrationManager = await OrchestrationSessionPartitionHandler.GetOrchestrationSessionManagerAsync(this.redisConnection, this.settings.TaskHubName);
                this.activityTaskManager = new ActivityTaskHandler(taskHub: this.settings.TaskHubName, workerId: this.workerGuid, connection: this.redisConnection);
                this.logger = new RedisLogger(this.redisConnection, this.settings.TaskHubName);
            }

            this.startLock.Release();
        }

        private void RegisterWorker()
        {
            IDatabase database = this.redisConnection.GetDatabase();
            string workerSetKey = RedisKeyNameResolver.GetWorkerSetKey(this.settings.TaskHubName);
            database.SetAdd(workerSetKey, this.workerGuid);

            // TODO: Set up ping background job for multi-worker scenario
        }

        /// <inheritdoc />
        public async Task StopAsync()
        {
            if (this.redisConnection != null)
            {
                await this.redisConnection.CloseAsync();
            }
        }

        /// <inheritdoc />
        public async Task StopAsync(bool isForced)
        {
            await this.StopAsync();
        }
        #endregion

        #region Dispatcher Methods
        /// <inheritdoc />
        public int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            return 0;
        }

        /// <inheritdoc />
        public int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            return 0;
        }
        #endregion

        #region Client Methods
        /// <inheritdoc />
        public async Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            if (this.partitionOrchestrationManager == null)
            {
                await StartAsync();
            }
            await this.partitionOrchestrationManager.CreateTaskOrchestration(creationMessage, null);
        }

        /// <inheritdoc />
        public async Task CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
        {
            if (this.partitionOrchestrationManager == null)
            {
                await StartAsync();
            }
            await this.partitionOrchestrationManager.CreateTaskOrchestration(creationMessage, dedupeStatuses);
        }

        /// <inheritdoc />
        public async Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            if (this.partitionOrchestrationManager == null)
            {
                await StartAsync();
            }
            await SendTaskOrchestrationMessageBatchAsync(message);
        }

        /// <inheritdoc />
        public async Task SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
        {
            if (this.partitionOrchestrationManager == null)
            {
                await StartAsync();
            }
            await this.partitionOrchestrationManager.SendOrchestrationMessages(messages);
        }

        /// <inheritdoc />
        public async Task<OrchestrationState> WaitForOrchestrationAsync(
            string instanceId,
            string executionId,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            if (this.partitionOrchestrationManager == null)
            {
                await StartAsync();
            }

            if (string.IsNullOrWhiteSpace(instanceId))
            {
                throw new ArgumentException(nameof(instanceId));
            }

            var timeoutCancellationSource = new CancellationTokenSource(timeout);
            var globalCancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCancellationSource.Token);
            var globalCancellationToken = globalCancellationSource.Token;

            TimeSpan statusPollingInterval = TimeSpan.FromMilliseconds(250);
            while (!globalCancellationToken.IsCancellationRequested)
            {
                OrchestrationState state = await this.GetOrchestrationStateAsync(instanceId, executionId);
                if (state == null ||
                    state.OrchestrationStatus == OrchestrationStatus.Running ||
                    state.OrchestrationStatus == OrchestrationStatus.Pending ||
                    state.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew)
                {
                    await Task.Delay(statusPollingInterval, globalCancellationToken);
                }
                else
                {
                    return state;
                }
            }

            return null;
        }

        /// <inheritdoc />
        public Task ForceTerminateTaskOrchestrationAsync(string instanceId, string reason)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        {
            return new OrchestrationState[] { await this.GetOrchestrationStateAsync(instanceId, "") };
        }

        /// <inheritdoc />
        public async Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            if (this.partitionOrchestrationManager == null)
            {
                await StartAsync();
            }
            return await this.partitionOrchestrationManager.GetOrchestrationState(instanceId);
        }

        /// <inheritdoc />
        public Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public Task PurgeOrchestrationHistoryAsync(
            DateTime thresholdDateTimeUtc,
            OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}
