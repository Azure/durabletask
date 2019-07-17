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

namespace DurableTask.EventHubs
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using Microsoft.Azure.EventHubs.Processor;
    using Microsoft.Azure.EventHubs;

    public class EventHubsOrchestrationService : IOrchestrationService
    {
        readonly BlockingCollection<ConcurrentQueue<int>> foo;
        readonly EventHubsOrchestrationServiceSettings settings;
        readonly EventProcessorHost host;
        readonly OrchestrationWorkItemProducer orchestrationWorkItemProvider;

        public EventHubsOrchestrationService(EventHubsOrchestrationServiceSettings settings)
        {
            this.settings = settings ?? throw new ArgumentNullException(nameof(settings));

            this.host = new EventProcessorHost(
                settings.TaskHubName,
                PartitionReceiver.DefaultConsumerGroupName,
                settings.EventHubsConnectionString,
                settings.StorageConnectionString,
                settings.LeaseContainerName);
        }

        public int TaskOrchestrationDispatcherCount => throw new NotImplementedException();

        public int MaxConcurrentTaskOrchestrationWorkItems => throw new NotImplementedException();

        public BehaviorOnContinueAsNew EventBehaviourForContinueAsNew => throw new NotImplementedException();

        public int TaskActivityDispatcherCount => throw new NotImplementedException();

        public int MaxConcurrentTaskActivityWorkItems => throw new NotImplementedException();

        public async Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            try
            {
                return await this.orchestrationWorkItemProvider.TakeAsync(cancellationToken);
            }
            catch (OperationCanceledException e) when (e.CancellationToken == cancellationToken)
            {
                return null;
            }
        }

        public Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
        {
            throw new NotImplementedException();
        }

        public Task CompleteTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem, OrchestrationRuntimeState newOrchestrationRuntimeState, IList<TaskMessage> outboundMessages, IList<TaskMessage> orchestratorMessages, IList<TaskMessage> timerMessages, TaskMessage continuedAsNewMessage, OrchestrationState orchestrationState)
        {
            throw new NotImplementedException();
        }

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

        public int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            throw new NotImplementedException();
        }

        public int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            throw new NotImplementedException();
        }

        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            throw new NotImplementedException();
        }

        public Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public async Task StartAsync()
        {
            await this.host.RegisterEventProcessorAsync<OrchestrationEventProcessor>();
        }

        public async Task StopAsync()
        {
            await this.host.UnregisterEventProcessorAsync();
        }

        public async Task StopAsync(bool isForced)
        {
            await this.host.UnregisterEventProcessorAsync();
        }
    }
}
