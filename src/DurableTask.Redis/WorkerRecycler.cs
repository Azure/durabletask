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
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;
using StackExchange.Redis;
using static StackExchange.Redis.RedisChannel;

namespace DurableTask.Redis
{
    /// <summary>
    /// Responsible for cleaning up the processing queues and other Redis data of dead workers. Right now,
    /// since only one worker can be alive at a time, as long as this finishes cleaning up before 
    /// a new activity worker spins up, it can be done on startup once, and forgotten about. In the future, 
    /// workers will have to constantly compete to be the "cleaner", and this class will be performing
    /// several background cleanup jobs to see if any workers have died recently and cleaning them up if necessary.
    /// </summary>
    internal class WorkerRecycler
    {
        private readonly string taskHub;
        private readonly ConnectionMultiplexer redisConnection;
        private readonly string workerSetKey;
        private readonly string incomingActivityQueueKey;
        private readonly RedisLogger logger;

        public WorkerRecycler(string taskHub,  ConnectionMultiplexer connection)
        {
            this.taskHub = taskHub;
            this.redisConnection = connection;
            this.workerSetKey = RedisKeyNameResolver.GetWorkerSetKey(this.taskHub);
            this.incomingActivityQueueKey = RedisKeyNameResolver.GetTaskActivityIncomingQueueKey(this.taskHub);
            this.logger = new RedisLogger(this.redisConnection, this.taskHub);
        }

        /// <summary>
        /// For now this class just cleans up all workers on the task hub, but eventually it will run in the background
        /// and clean up any workers that haven't proven they are alive in the last n seconds. For now it must be called BEFORE
        /// creating a new <see cref="ActivityTaskHandler"/>.
        /// </summary>
        /// <returns></returns>
        public async Task CleanupWorkersAsync()
        {
            IDatabase redisDatabase = this.redisConnection.GetDatabase();
            RedisValue[] deadWorkerIds = await redisDatabase.SetMembersAsync(this.workerSetKey);
            
            foreach (string deadWorkerId in deadWorkerIds)
            {
                string processingQueueKey = RedisKeyNameResolver.GetTaskActivityProcessingQueueKey(this.taskHub, deadWorkerId);
                long itemsToRestore = await redisDatabase.ListLengthAsync(processingQueueKey);
                await this.logger.LogAsync($"Moving {itemsToRestore} from processing queue back to incoming queue");
                string restoredMessage = await redisDatabase.ListRightPopLeftPushAsync(processingQueueKey, incomingActivityQueueKey);
                while (restoredMessage != null)
                {
                    TaskMessage message = RedisSerializer.DeserializeObject<TaskMessage>(restoredMessage);
                    await this.logger.LogAsync($"Moved activity with id {message.Event.EventId} from processing queue back to incoming queue");
                    restoredMessage = await redisDatabase.ListRightPopLeftPushAsync(processingQueueKey, incomingActivityQueueKey);
                }
            }
        }
    }
}
