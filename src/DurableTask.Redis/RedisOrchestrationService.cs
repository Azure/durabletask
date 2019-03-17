﻿//  ----------------------------------------------------------------------------------
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
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;

    public class RedisOrchestrationService : IOrchestrationService, IOrchestrationServiceClient
    {
        readonly RedisOrchestrationServiceSettings settings;

        public RedisOrchestrationService(RedisOrchestrationServiceSettings settings)
        {
            this.settings = RedisOrchestrationServiceSettings.Validate(settings);
        }

        public int TaskOrchestrationDispatcherCount => 1;

        public int MaxConcurrentTaskOrchestrationWorkItems => this.settings.MaxConcurrentTaskOrchestrationWorkItems;

        public BehaviorOnContinueAsNew EventBehaviourForContinueAsNew => BehaviorOnContinueAsNew.Carryover;

        public int TaskActivityDispatcherCount => 1;

        public int MaxConcurrentTaskActivityWorkItems => this.settings.MaxConcurrentTaskActivityWorkItems;


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
            throw new NotImplementedException();
        }

        public Task CreateAsync(bool recreateInstanceStore)
        {
            throw new NotImplementedException();
        }

        public Task CreateIfNotExistsAsync()
        {
            throw new NotImplementedException();
        }

        public Task DeleteAsync()
        {
            throw new NotImplementedException();
        }

        public Task DeleteAsync(bool deleteInstanceStore)
        {
            throw new NotImplementedException();
        }

        public Task StartAsync()
        {
            throw new NotImplementedException();
        }

        public Task StopAsync()
        {
            throw new NotImplementedException();
        }

        public Task StopAsync(bool isForced)
        {
            throw new NotImplementedException();
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
