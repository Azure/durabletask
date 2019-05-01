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
using System.Threading.Tasks;
using DurableTask.Core;
using StackExchange.Redis;

namespace DurableTask.Redis
{
    /// <summary>
    /// Allows chaining Redis commands into a single transaction.
    /// </summary>
    internal class RedisTransactionBuilder
    {
        private readonly string taskHub;
        private readonly string partition;
        private readonly ITransaction transaction;
        private readonly IConnectionMultiplexer connection;

        private RedisTransactionBuilder(string taskHub, string partition, ConnectionMultiplexer connection, Condition condition)
        {
            this.taskHub = taskHub;
            this.partition = partition;
            this.connection = connection;
            this.transaction = connection.GetDatabase().CreateTransaction();
            this.transaction.AddCondition(condition);
        }

        public static RedisTransactionBuilder BuildTransactionInPartition(string taskHub, string partition, ConnectionMultiplexer connection, Condition condition)
        {
            return new RedisTransactionBuilder(taskHub, partition, connection, condition);
        }

        public static RedisTransactionBuilder BuildTransaction(string taskHub, ConnectionMultiplexer connection, Condition condition)
        {
            return new RedisTransactionBuilder(taskHub, null, connection, condition);
        }

        public RedisTransactionBuilder SendControlQueueMessage(TaskMessage message)
        {
            if (this.partition == null)
            {
                throw new ArgumentNullException($"Cannot call {nameof(SendControlQueueMessage)} without a partition set.");
            }
            string controlQueueKey = RedisKeyNameResolver.GetPartitionControlQueueKey(this.taskHub, this.partition);
            string controlQueueNotificationChannelKey = RedisKeyNameResolver.GetPartitionControlNotificationChannelKey(this.taskHub, this.partition);
            string messageJson = RedisSerializer.SerializeObject(message);
            this.transaction.ListLeftPushAsync(controlQueueKey, messageJson);
            this.transaction.PublishAsync(new RedisChannel(controlQueueNotificationChannelKey, RedisChannel.PatternMode.Literal), messageJson);
            return this;
        }

        public RedisTransactionBuilder SetOrchestrationRuntimeState(string orchestrationId, OrchestrationRuntimeState state)
        {
            if (this.partition == null)
            {
                throw new ArgumentNullException($"Cannot call {nameof(SetOrchestrationRuntimeState)} without a partition set.");
            }
            string orchestrationStateKey = RedisKeyNameResolver.GetOrchestrationRuntimeStateHashKey(this.taskHub, this.partition);
            // Do not want to serialize new events;
            state.NewEvents.Clear();
            string stateJson = RedisSerializer.SerializeObject(state.Events);
            transaction.HashSetAsync(orchestrationStateKey, orchestrationId, stateJson);
            return this;
        }

        public RedisTransactionBuilder AddOrchestrationToSet(string orchestrationId)
        {
            if (this.partition == null)
            {
                throw new ArgumentNullException($"Cannot call {nameof(AddOrchestrationToSet)} without a partition set.");
            }
            string orchestrationStateKey = RedisKeyNameResolver.GetOrchestrationsSetKey(this.taskHub, this.partition);
            transaction.SetAddAsync(orchestrationStateKey, orchestrationId);
            return this;
        }

        public RedisTransactionBuilder AddOrchestrationStateToHistory(string orchestrationId, OrchestrationState state)
        {
            if (this.partition == null)
            {
                throw new ArgumentNullException($"Cannot call {nameof(AddOrchestrationStateToHistory)} without a partition set.");
            }
            string orchestrationStateKey = RedisKeyNameResolver.GetOrchestrationStateKey(this.taskHub, this.partition, orchestrationId);
            string stateJson = RedisSerializer.SerializeObject(state);
            // Add to front of list so history in latest to oldest order
            transaction.StringSetAsync(orchestrationStateKey, stateJson);
            return this;
        }

        public RedisTransactionBuilder SendActivityMessage(TaskMessage message)
        {
            string activityIncomingQueueKey = RedisKeyNameResolver.GetTaskActivityIncomingQueueKey(this.taskHub);
            string messageJson = RedisSerializer.SerializeObject(message);
            transaction.ListLeftPushAsync(activityIncomingQueueKey, messageJson);
            return this;
        }

        public RedisTransactionBuilder RemoveTaskMessageFromActivityQueue(TaskMessage message, string workerId)
        {
            string activityProcessingQueueKey = RedisKeyNameResolver.GetTaskActivityProcessingQueueKey(this.taskHub, workerId);
            string messageJson = RedisSerializer.SerializeObject(message);
            transaction.ListRemoveAsync(activityProcessingQueueKey, messageJson);
            return this;
        }

        public RedisTransactionBuilder RemoveItemsFromOrchestrationQueue(string orchestrationId, int numberOfItemsToRemove)
        {
            if (this.partition == null)
            {
                throw new ArgumentNullException($"Cannot call {nameof(RemoveItemsFromOrchestrationQueue)} without a partition set.");
            }
            string orchestrationQueueKey = RedisKeyNameResolver.GetOrchestrationQueueKey(this.taskHub, this.partition, orchestrationId);
            for (int i = 0; i < numberOfItemsToRemove; i++)
            {
                transaction.ListRightPopAsync(orchestrationQueueKey);
            }
            
            return this;
        }

        public async Task<bool> CommitTransactionAsync()
        {
            return await transaction.ExecuteAsync();
        }
    }
}
