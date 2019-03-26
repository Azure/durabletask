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
    using StackExchange.Redis;

    public class RedisOrchestrationService : IOrchestrationService, IOrchestrationServiceClient
    {
        private readonly RedisOrchestrationServiceSettings settings;
        private ConnectionMultiplexer internalRedisConnection;

        public RedisOrchestrationService(RedisOrchestrationServiceSettings settings)
        {
            this.settings = RedisOrchestrationServiceSettings.Validate(settings);
        }

        public int TaskOrchestrationDispatcherCount => 1;

        public int MaxConcurrentTaskOrchestrationWorkItems => this.settings.MaxConcurrentTaskOrchestrationWorkItems;

        public BehaviorOnContinueAsNew EventBehaviourForContinueAsNew => BehaviorOnContinueAsNew.Carryover;

        public int TaskActivityDispatcherCount => 1;

        public int MaxConcurrentTaskActivityWorkItems => this.settings.MaxConcurrentTaskActivityWorkItems;

        private async Task<ConnectionMultiplexer> GetRedisConnection()
        {
            if (this.internalRedisConnection == null)
            {
                this.internalRedisConnection = await ConnectionMultiplexer.ConnectAsync(this.settings.RedisConnectionString);
            }
            return this.internalRedisConnection;
        }

        #region Orchestration Methods
        public Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            return false;
        }

        public Task CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState,
            IList<TaskMessage> outboundMessages,
            IList<TaskMessage> orchestratorMessages,
            IList<TaskMessage> timerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState orchestrationState)
        {
            throw new NotImplementedException();
        }

        public Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            throw new NotImplementedException();
        }
        #endregion

        #region Activity Methods
        public Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task CompleteTaskActivityWorkItemAsync(
            TaskActivityWorkItem workItem,
            TaskMessage responseMessage)
        {
            throw new NotImplementedException();
        }

        public Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            throw new NotImplementedException();
        }
        #endregion

        #region Task Hub Methods
        public Task CreateAsync()
        {
            // No operation needed, Redis objects are created on the fly if necessary.
            return Task.CompletedTask;
        }

        public Task CreateAsync(bool recreateInstanceStore)
        {
            // No operation needed, Redis objects are created on the fly if necessary.
            return Task.CompletedTask;
        }

        public Task CreateIfNotExistsAsync()
        {
            // No operation needed, Redis objects are created on the fly if necessary.
            return Task.CompletedTask;
        }

        public async Task DeleteAsync()
        {
            ConnectionMultiplexer redisConnection = await this.GetRedisConnection();
            IDatabase db = redisConnection.GetDatabase();
            EndPoint[] endpoints = redisConnection.GetEndPoints();
            IServer keyServer;
            if (endpoints.Length == 1)
            {
                // Grab the keys from the only server
                keyServer = redisConnection.GetServer(endpoints[0]);
            }
            else
            {
                // Grab the keys from a slave endpoint to avoid hitting the master too hard with the
                // expensive request. If no slaves are available, just grab the first available server
                IServer[] servers = endpoints.Select(ep => redisConnection.GetServer(ep)).ToArray();
                keyServer = servers.FirstOrDefault(server => server.IsSlave && server.IsConnected)
                    ?? servers.FirstOrDefault(server => server.IsConnected)
                    ?? throw new InvalidOperationException("None of the Redis servers are connected.");
            }
            // Grab all keys that have the task hub prefix
            RedisKey[] keysToDelete = keyServer.Keys(pattern: $"{this.settings.TaskHubName}.*").ToArray();
            await db.KeyDeleteAsync(keysToDelete);
        }

        public async Task DeleteAsync(bool deleteInstanceStore)
        {
            await this.DeleteAsync();
        }

        public Task StartAsync()
        {
            // TODO: start listening on reliable queue for events
            return Task.CompletedTask;
        }

        public async Task StopAsync()
        {
            ConnectionMultiplexer redisConnection = await this.GetRedisConnection();
            await redisConnection.CloseAsync();
        }

        public async Task StopAsync(bool isForced)
        {
            await this.StopAsync();
        }
        #endregion

        #region Dispatcher Methods
        public int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            throw new NotImplementedException();
        }

        public int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            throw new NotImplementedException();
        }
        #endregion

        #region Client Methods
        public Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            throw new NotImplementedException();
        }

        public Task CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
        {
            throw new NotImplementedException();
        }

        public Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            throw new NotImplementedException();
        }

        public Task SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
        {
            throw new NotImplementedException();
        }

        public Task<OrchestrationState> WaitForOrchestrationAsync(
            string instanceId,
            string executionId,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task ForceTerminateTaskOrchestrationAsync(string instanceId, string reason)
        {
            throw new NotImplementedException();
        }

        public Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        {
            throw new NotImplementedException();
        }

        public Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }

        public Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }

        public Task PurgeOrchestrationHistoryAsync(
            DateTime thresholdDateTimeUtc,
            OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}
