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

namespace DurableTask.Redis
{
    /// <summary>
    /// Responsible for taking items off of the incoming processing queue and reserving them to process them.
    /// Right now only one is alive, but when multiple are alive, they will have to have a background task to constantly
    /// add some Redis state to prove they are still alive
    /// </summary>
    internal class ActivityTaskHandler
    {
        //TODO: Eventually these will need to be Redis locks once we have a multiple worker scenario
        private readonly SemaphoreSlim incomingQueueLock = new SemaphoreSlim(1);

        //TODO: This locking approach currently has a race condition that allows an activity task to be called multiple
        // times. Will likely require a somewhat substantial redesign to fix.
        private readonly ConcurrentDictionary<int, SemaphoreSlim> taskLocks;

        private readonly string workerId;
        private readonly string taskHub;
        private readonly ConnectionMultiplexer redisConnection;
        private readonly string processingTaskQueueKey;
        private readonly string incomingTaskQueueKey;
        private readonly RedisLogger redisLogger;

        public ActivityTaskHandler(string taskHub, string workerId, ConnectionMultiplexer connection)
        {
            this.workerId = workerId;
            this.taskHub = taskHub;
            this.taskLocks = new ConcurrentDictionary<int, SemaphoreSlim>();
            this.redisConnection = connection;
            this.processingTaskQueueKey = RedisKeyNameResolver.GetTaskActivityProcessingQueueKey(this.taskHub, this.workerId);
            this.incomingTaskQueueKey = RedisKeyNameResolver.GetTaskActivityIncomingQueueKey(this.taskHub);
            this.redisLogger = new RedisLogger(this.redisConnection, this.taskHub);
        }

        public async Task<TaskActivityWorkItem> GetSingleTaskItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            await this.redisLogger.LogAsync($"Attempting to grab an activity task item for {receiveTimeout}");
            var timeoutCancellationSource = new CancellationTokenSource(receiveTimeout);
            var globalCancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCancellationSource.Token);
            var globalCancellationToken = globalCancellationSource.Token;

            IDatabase redisDb = this.redisConnection.GetDatabase();
            int numTries = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
                string messageJson = await redisDb.ListRightPopLeftPushAsync(this.incomingTaskQueueKey, this.processingTaskQueueKey);
                if (messageJson == null)
                {
                    numTries += 1;
                    await Task.Delay(50);
                    continue;
                }

                await this.redisLogger.LogAsync($"Got activity task item after {numTries} attempts");
                TaskMessage message = RedisSerializer.DeserializeObject<TaskMessage>(messageJson);
                return new TaskActivityWorkItem
                {
                    Id = Guid.NewGuid().ToString(),
                    LockedUntilUtc = DateTime.UtcNow.AddHours(1), // technically it is locked indefinitely
                    TaskMessage = message,
                };
            }

            // If task cancelled due to timeout or cancellation token, just return null
            return null;
        }
    }
}
