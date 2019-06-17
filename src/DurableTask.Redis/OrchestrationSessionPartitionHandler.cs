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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.Exceptions;
using DurableTask.Core.History;
using StackExchange.Redis;

namespace DurableTask.Redis
{
    /// <summary>
    /// Responsible for processing orchestration sessions for a given orchestration partition. At this time,
    /// there is only one orchestration partition due to only one worker existing at a time, but in the future
    /// partition management and assigning these partitions to workers will be required.
    /// </summary>
    internal class OrchestrationSessionPartitionHandler
    {
        // TODO: Don't need to be redis locks since only one worker can have a partition at a time. However,
        // will need a single redis lock to enforce only one worker having access to a partition.
        private readonly ConcurrentDictionary<string, SemaphoreSlim> activeOrchestrationLocks;
        private readonly SemaphoreSlim controlQueueLock = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim orchestrationCurrentStateLock = new SemaphoreSlim(1,1);

        private readonly string taskHub;
        private readonly ConnectionMultiplexer redisConnection;
        private readonly string partition;
        private readonly string partitionControlQueueKey;
        private readonly string currentOrchestrationsSetKey;
        private readonly string partitionControlQueueNotificationChannelKey;
        private readonly RedisLogger logger;

        private OrchestrationSessionPartitionHandler(ConnectionMultiplexer redisConnection, string taskHub, string partition = "singleton")
        {
            this.taskHub = taskHub;
            this.redisConnection = redisConnection;
            this.partition = partition;
            this.activeOrchestrationLocks = new ConcurrentDictionary<string, SemaphoreSlim>();
            this.partitionControlQueueKey = RedisKeyNameResolver.GetPartitionControlQueueKey(this.taskHub, this.partition);
            this.partitionControlQueueNotificationChannelKey = RedisKeyNameResolver.GetPartitionControlNotificationChannelKey(this.taskHub, this.partition);
            this.currentOrchestrationsSetKey = RedisKeyNameResolver.GetOrchestrationsSetKey(this.taskHub, this.partition);
            this.logger = new RedisLogger(redisConnection, taskHub);
        }

        public static async Task<OrchestrationSessionPartitionHandler> GetOrchestrationSessionManagerAsync(ConnectionMultiplexer redisConnection, string taskHub, string partition = "singleton")
        {
            OrchestrationSessionPartitionHandler sessionManager = new OrchestrationSessionPartitionHandler(redisConnection, taskHub, partition);
            await sessionManager.InitializeAsync();
            return sessionManager;
        }

        // Update in memory solution
        private async Task InitializeAsync()
        {
            await RestoreLocks();
            await SubscribeToMainControlQueue();
        }

        public async Task<TaskOrchestrationWorkItem> GetSingleOrchestrationSessionAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            Stopwatch timer = Stopwatch.StartNew();
            int orchestrationCount = activeOrchestrationLocks.Keys.Count;
            if (orchestrationCount == 0)
            {
                return null;
            }

            string[] orchestrationIds = activeOrchestrationLocks.Keys.ToArray();
            int currentIndex = 0;
            int count = 0;
            int perOrchestrationWaitTime = 5; // No particular reason for this value. 
            while (!((int)timer.ElapsedMilliseconds > receiveTimeout.Milliseconds && cancellationToken.IsCancellationRequested))
            {
                string orchestrationId = orchestrationIds[currentIndex];
                //await this.logger.LogAsync($"Attempting to grab an orchestration task item for orchestration {orchestrationId}");
                bool gotOrchestrationLock = await activeOrchestrationLocks[orchestrationId].WaitAsync(perOrchestrationWaitTime);
                if (gotOrchestrationLock)
                {
                    List<TaskMessage> queuedMessages = await this.GetOrchestrationControlQueueMessages(orchestrationId);
                    if (queuedMessages.Count > 0)
                    {
                        //await this.logger.LogAsync($"Taking lock on {orchestrationId} after {count} attempts");
                        OrchestrationRuntimeState runtimeState = await GetOrchestrationRuntimeState(orchestrationId);
                        return new TaskOrchestrationWorkItem()
                        {
                            InstanceId = orchestrationId,
                            NewMessages = queuedMessages,
                            LockedUntilUtc = DateTime.UtcNow.AddHours(1), // technically it is locked indefinitely
                            OrchestrationRuntimeState = runtimeState
                        };
                    }
                    else
                    {
                        //await this.logger.LogAsync($"Nothing in queue for orchestration {orchestrationId}");
                    }

                    // Release lock and try another
                    activeOrchestrationLocks[orchestrationId].Release();
                }
                else
                {
                    await this.logger.LogAsync($"Failed to get lock on {orchestrationId}");
                }

                currentIndex += 1;
                count += 1;
                if (currentIndex == orchestrationIds.Length)
                {
                    // reached the end of the keys, try again from the beginning
                    currentIndex = 0;
                }
            }

            return null;
        }

        public void ReleaseOrchestration(string orchestrationId)
        {
            if (this.activeOrchestrationLocks.TryGetValue(orchestrationId, out SemaphoreSlim orchLock))
            {
                this.activeOrchestrationLocks[orchestrationId].Release();
            }    
        }
        
        public async Task CreateTaskOrchestration(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
        {
            if (!(creationMessage.Event is ExecutionStartedEvent executionStartEvent))
            {
                throw new InvalidOperationException("Invalid creation task message");
            }
            string orchestrationId = creationMessage.OrchestrationInstance.InstanceId;

            // Lock so that two clients can't create the same orchestrationId at the same time.
            await this.orchestrationCurrentStateLock.WaitAsync();

            IDatabase redisDB = this.redisConnection.GetDatabase();
            string orchestrationHistoryKey = RedisKeyNameResolver.GetOrchestrationStateKey(this.taskHub, this.partition, orchestrationId); 
            string currentStateJson = await redisDB.StringGetAsync(orchestrationHistoryKey);
            
            if (currentStateJson != null)
            {
                OrchestrationState currentState = RedisSerializer.DeserializeObject<OrchestrationState>(currentStateJson);
                if (dedupeStatuses == null || dedupeStatuses.Contains(currentState.OrchestrationStatus))
                {
                    // An orchestration with same instance id is already running
                    throw new OrchestrationAlreadyExistsException($"An orchestration with id '{creationMessage.OrchestrationInstance.InstanceId}' already exists. It is in state {currentState.OrchestrationStatus}");
                }
            }

            RedisTransactionBuilder transactionBuilder = this.CreateNonExistingOrchestrationTransaction(orchestrationId);
            var state = new OrchestrationState
            {
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = creationMessage.OrchestrationInstance.InstanceId,
                    ExecutionId = creationMessage.OrchestrationInstance.ExecutionId,
                },
                CreatedTime = DateTime.UtcNow,
                OrchestrationStatus = OrchestrationStatus.Pending,
                Version = executionStartEvent.Version,
                Name = executionStartEvent.Name,
                Input = executionStartEvent.Input,
            };
            transactionBuilder.AddOrchestrationToSet(orchestrationId);
            transactionBuilder.AddOrchestrationStateToHistory(orchestrationId, state);
            transactionBuilder.SendControlQueueMessage(creationMessage);
            bool transactionSucceeded = await transactionBuilder.CommitTransactionAsync();

            string logMessage;
            if (transactionSucceeded)
            {
                logMessage = "Succeeded in transaction of creating task orchestration";
            }
            else
            {
                logMessage = "Failed in transaction of creating task orchestration";
            }
            await this.logger.LogAsync(logMessage);

            this.orchestrationCurrentStateLock.Release();
        }

        public async Task SendOrchestrationMessages(params TaskMessage[] messages)
        {
            IDatabase redisDb = this.redisConnection.GetDatabase();
            Task[] sendMessageTasks = new Task[messages.Length];
            RedisChannel notificationChannel = new RedisChannel(this.partitionControlQueueNotificationChannelKey, RedisChannel.PatternMode.Literal);
            for (int i = 0; i < messages.Length; i++)
            {
                string messageJson = RedisSerializer.SerializeObject(messages[i]);
                sendMessageTasks[i] = redisDb.ListLeftPushAsync(this.partitionControlQueueKey, messageJson);
            }
            await Task.WhenAll(sendMessageTasks);
            await redisDb.PublishAsync(notificationChannel, "batch received");
        }

        public RedisTransactionBuilder CreateExistingOrchestrationTransaction(string orchestrationId)
        {
            IDatabase redisDatabase = this.redisConnection.GetDatabase();
            return RedisTransactionBuilder.BuildTransactionInPartition(
                taskHub: this.taskHub,
                partition: this.partition,
                connection: this.redisConnection, 
                condition: Condition.SetContains(this.currentOrchestrationsSetKey, orchestrationId));
        }

        public RedisTransactionBuilder CreateNonExistingOrchestrationTransaction(string orchestrationId)
        {
            IDatabase redisDatabase = this.redisConnection.GetDatabase();
            return RedisTransactionBuilder.BuildTransactionInPartition(
                taskHub: this.taskHub,
                partition: this.partition,
                connection: this.redisConnection,
                condition: Condition.SetNotContains(this.currentOrchestrationsSetKey, orchestrationId));
        }

        public async Task<OrchestrationState> GetOrchestrationState(string orchestrationId)
        {
            IDatabase redisDatabase = this.redisConnection.GetDatabase();
            string orchestrationStateKey = RedisKeyNameResolver.GetOrchestrationStateKey(this.taskHub, this.partition, orchestrationId);
            string orchestrationStateJson = await redisDatabase.StringGetAsync(orchestrationStateKey);
            if (orchestrationStateJson == null)
            {
                return null;
            }
            return RedisSerializer.DeserializeObject<OrchestrationState>(orchestrationStateJson);
        }

        private async Task<List<TaskMessage>> GetOrchestrationControlQueueMessages(string orchestrationId)
        {
            IDatabase redisDatabase = this.redisConnection.GetDatabase();
            string orchestrationControlQueueKey = RedisKeyNameResolver.GetOrchestrationQueueKey(this.taskHub, this.partition, orchestrationId);
            return (await redisDatabase.ListRangeAsync(orchestrationControlQueueKey))
                .Select(json => RedisSerializer.DeserializeObject<TaskMessage>(json))
                .ToList();
        }

        private async Task<OrchestrationRuntimeState> GetOrchestrationRuntimeState(string orchestrationId)
        {
            IDatabase redisDatabase = this.redisConnection.GetDatabase();
            string orchestrationRuntimeState = RedisKeyNameResolver.GetOrchestrationRuntimeStateHashKey(this.taskHub, this.partition);
            string eventListJson = await redisDatabase.HashGetAsync(orchestrationRuntimeState, orchestrationId);
            return RedisSerializer.DeserializeRuntimeState(eventListJson);
        }

        private async Task SubscribeToMainControlQueue()
        {
            IDatabase redisDatabase = this.redisConnection.GetDatabase();
            RedisChannel controlQueueChannel = new RedisChannel(this.partitionControlQueueNotificationChannelKey, RedisChannel.PatternMode.Literal);
            ISubscriber subscriber = this.redisConnection.GetSubscriber();
            subscriber.Subscribe(controlQueueChannel).OnMessage(async _ =>
            {
                await ProcessQueueItemsAsync();
            });

            // Call once just to process all of the items that haven't been handled yet.
            await ProcessQueueItemsAsync();
        }

        private async Task ProcessQueueItemsAsync()
        {
            await this.controlQueueLock.WaitAsync();

            IDatabase database = this.redisConnection.GetDatabase();
            RedisValue[] messagesJson = await database.ListRangeAsync(this.partitionControlQueueKey);

            foreach(string messageJson in messagesJson)
            {
                TaskMessage taskMessage = RedisSerializer.DeserializeObject<TaskMessage>(messageJson);
                string instanceId = taskMessage.OrchestrationInstance.InstanceId;

                string orchestrationStateKey = RedisKeyNameResolver.GetOrchestrationRuntimeStateHashKey(this.taskHub, this.partition);
                string orchestrationInstanceQueueKey = RedisKeyNameResolver.GetOrchestrationQueueKey(this.taskHub, this.partition, instanceId);

                // Enter empty runtime state if it is an execution start event
                if (taskMessage.Event is ExecutionStartedEvent startedEvent)
                {
                    OrchestrationRuntimeState emptyState = GetEmptyState(taskMessage);
                    await database.HashSetAsync(orchestrationStateKey, instanceId, RedisSerializer.SerializeObject(emptyState.Events), When.NotExists);
                }

                await database.ListRightPopLeftPushAsync(this.partitionControlQueueKey, orchestrationInstanceQueueKey);
                this.activeOrchestrationLocks.TryAdd(instanceId, new SemaphoreSlim(1,1));
            }

            //Release lock so another notification can be handled
            this.controlQueueLock.Release();
        }

        private OrchestrationRuntimeState GetEmptyState(TaskMessage message)
        {
            var orchestrationState = new OrchestrationRuntimeState();
            orchestrationState.AddEvent(message.Event);
            return orchestrationState;
        }

        private async Task RestoreLocks()
        {
            IDatabase redisDatabase = this.redisConnection.GetDatabase();
            string runtimeEventsKey = RedisKeyNameResolver.GetOrchestrationRuntimeStateHashKey(this.taskHub, this.partition);
            HashEntry[] runtimeEvents = await redisDatabase.HashGetAllAsync(runtimeEventsKey);
            foreach (var hashEntry in runtimeEvents)
            {
                OrchestrationRuntimeState state = RedisSerializer.DeserializeRuntimeState(hashEntry.Value);
                if (state.OrchestrationStatus == OrchestrationStatus.Pending ||
                    state.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew ||
                    state.OrchestrationStatus == OrchestrationStatus.Running)
                {
                    // Still running, so add lock
                    this.activeOrchestrationLocks[hashEntry.Name] = new SemaphoreSlim(1);
                }
            }
        }
    }
}
