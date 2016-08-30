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
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Data;
    using Microsoft.ServiceFabric.Data.Collections;

    class ActivitiesProvider
    {
        IReliableStateManager stateManager;
        IReliableQueue<TaskMessage> activityQueue;

        public ActivitiesProvider(IReliableStateManager stateManager, IReliableQueue<TaskMessage> activityQueue)
        {
            if (stateManager == null)
            {
                throw new ArgumentNullException(nameof(stateManager));
            }

            if (activityQueue == null)
            {
                throw new ArgumentNullException(nameof(activityQueue));
            }

            this.stateManager = stateManager;
            this.activityQueue = activityQueue;
        }

        public async Task<TaskMessage> GetNextWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            Stopwatch timer = Stopwatch.StartNew();
            while (timer.Elapsed < receiveTimeout && !cancellationToken.IsCancellationRequested)
            {
                using (var txn = this.stateManager.CreateTransaction())
                {
                    var activityValue = await this.activityQueue.TryPeekAsync(txn, LockMode.Default);
                    if (activityValue.HasValue)
                    {
                        return activityValue.Value;
                    }
                }
                await Task.Delay(100, cancellationToken);
            }
            return null;
        }

        //Todo: Should this use the same transaction instance as the above method?
        // Currently the second parameter is ignored in implementation because it's expected that
        // this method gets a call synchronously with the above method. When that changes, the implementation
        // has to change accordingly.
        public Task CompleteWorkItem(ITransaction transaction, TaskMessage message)
        {
            return this.activityQueue.TryDequeueAsync(transaction);
        }

        public async Task AppendBatch(ITransaction transaction, IList<TaskMessage> messages)
        {
            foreach (var message in messages)
            {
                await this.activityQueue.EnqueueAsync(transaction, message);
            }
        }
    }
}
