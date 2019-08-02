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
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Local partition of the distributed orchestration service.
    /// </summary>
    public class EventHubsOrchestrationService : 
        IOrchestrationService, 
        IOrchestrationServiceClient, 
        Backend.IHost,
        IDisposable
    {
        private readonly Backend.ITaskHub taskHub;
        private readonly EventHubsOrchestrationServiceSettings settings;
        private CancellationTokenSource serviceShutdownSource;
        private static readonly Task completedTask = Task.FromResult<object>(null);

        private const int MaxConcurrentWorkItems = 20;

        //internal Dictionary<uint, Partition> Partitions { get; private set; }
        internal Client Client { get; private set; }

        internal uint NumberPartitions { get; private set; }
        uint Backend.IHost.NumberPartitions { set => this.NumberPartitions = value; }

        internal WorkQueue<TaskActivityWorkItem> ActivityWorkItemQueue { get; private set; }
        internal WorkQueue<TaskOrchestrationWorkItem> OrchestrationWorkItemQueue { get; private set; }

        /// <summary>
        /// Creates a new instance of the OrchestrationService with default settings
        /// </summary>
        public EventHubsOrchestrationService(EventHubsOrchestrationServiceSettings settings)
        {
            this.settings = settings;

            if (settings.UseEmulatedBackend)
            {
                this.taskHub = new EmulatedBackend(this, settings);
            }
            else
            {
                this.taskHub = new EventHubsBackend(this, settings);
            }
        }

        /******************************/
        // management methods
        /******************************/

        async Task IOrchestrationService.CreateAsync()
        {
  
            await ((IOrchestrationService)this).CreateAsync(true);
        }

        async Task IOrchestrationService.CreateAsync(bool recreateInstanceStore)
        {
            if (await this.taskHub.ExistsAsync())
            {
                if (recreateInstanceStore)
                {
                    await this.taskHub.DeleteAsync();

                    await this.taskHub.CreateAsync();
                }
            }
            else
            {
                await this.taskHub.CreateAsync();
            }
        }

        async Task IOrchestrationService.CreateIfNotExistsAsync()
        {
            await ((IOrchestrationService)this).CreateAsync(false);
        }

        async Task IOrchestrationService.DeleteAsync()
        {
            await this.taskHub.DeleteAsync();
        }

        async Task IOrchestrationService.DeleteAsync(bool deleteInstanceStore)
        {
            await this.taskHub.DeleteAsync();
        }

        async Task IOrchestrationService.StartAsync()
        {
            System.Diagnostics.Debug.Assert(this.serviceShutdownSource == null, "Already started");

            this.serviceShutdownSource = new CancellationTokenSource();

            this.ActivityWorkItemQueue = new WorkQueue<TaskActivityWorkItem>(this.serviceShutdownSource.Token, SendNullResponses);
            this.OrchestrationWorkItemQueue = new WorkQueue<TaskOrchestrationWorkItem>(this.serviceShutdownSource.Token, SendNullResponses);

            await taskHub.StartAsync();

            System.Diagnostics.Debug.Assert(this.Client != null, "Backend should have added client");
        }

        private static void SendNullResponses<T>(IEnumerable<CancellableCompletionSource<T>> waiters) where T : class
        {
            foreach (var waiter in waiters)
            {
                waiter.TrySetResult(null);
            }
        }

        async Task IOrchestrationService.StopAsync(bool isForced)
        {
            if (this.serviceShutdownSource != null)
            {
                this.serviceShutdownSource.Cancel();
                this.serviceShutdownSource.Dispose();
                this.serviceShutdownSource = null;

                await this.taskHub.StopAsync();
            }
        }

        Task IOrchestrationService.StopAsync()
        {
            return ((IOrchestrationService) this).StopAsync(false);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (this.serviceShutdownSource != null)
            {
                this.serviceShutdownSource.Cancel();
                this.serviceShutdownSource.Dispose();
                this.serviceShutdownSource = null;

                this.taskHub.StopAsync();
            }
        }

        /// <summary>
        /// Computes the partition for the given instance.
        /// </summary>
        /// <param name="instanceId">The instance id.</param>
        /// <returns>The partition id.</returns>
        public uint GetPartitionId(string instanceId)
        {
            return Fnv1aHashHelper.ComputeHash(instanceId) % this.NumberPartitions;
        }

        /******************************/
        // host methods
        /******************************/

        Backend.IClient Backend.IHost.AddClient(Guid clientId, Backend.ISender batchSender)
        {
            System.Diagnostics.Debug.Assert(this.Client == null, "Backend should create only 1 client");

            this.Client = new Client(clientId, batchSender, this.serviceShutdownSource.Token);

            return this.Client;
        }

        Backend.IPartition Backend.IHost.AddPartition(uint partitionId, Backend.ISender batchSender)
        {
            var partition = new Partition(partitionId, this.GetPartitionId, batchSender, this.settings, this.ActivityWorkItemQueue, this.OrchestrationWorkItemQueue, this.serviceShutdownSource.Token);

            return partition;
        }

        void Backend.IHost.ReportError(string msg, Exception e)
        {
            System.Diagnostics.Trace.TraceError($"!!! {msg}: {e}");
        }


        /******************************/
        // client methods
        /******************************/

        Task IOrchestrationServiceClient.CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            return Client.CreateTaskOrchestrationAsync(
                this.GetPartitionId(creationMessage.OrchestrationInstance.InstanceId),
                creationMessage, 
                null);
        }

        Task IOrchestrationServiceClient.CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
        {
            return Client.CreateTaskOrchestrationAsync(
                this.GetPartitionId(creationMessage.OrchestrationInstance.InstanceId),
                creationMessage, 
                dedupeStatuses);
        }

        Task IOrchestrationServiceClient.SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            return Client.SendTaskOrchestrationMessageBatchAsync(
                this.GetPartitionId(message.OrchestrationInstance.InstanceId), 
                new[] { message });
        }

        Task IOrchestrationServiceClient.SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
        {
            if (messages.Length == 0)
            {
                return completedTask;
            }
            else
            {
                return Task.WhenAll(messages
                    .GroupBy(tm => this.GetPartitionId(tm.OrchestrationInstance.InstanceId))
                    .Select(group => Client.SendTaskOrchestrationMessageBatchAsync(group.Key, group)));
            }
        }

        Task<OrchestrationState> IOrchestrationServiceClient.WaitForOrchestrationAsync(
            string instanceId,
            string executionId,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            return Client.WaitForOrchestrationAsync(
                this.GetPartitionId(instanceId),
                instanceId, 
                executionId, 
                timeout, 
                cancellationToken);
        }

        async Task<OrchestrationState> IOrchestrationServiceClient.GetOrchestrationStateAsync(
            string instanceId, 
            string executionId)
        {
            var partitionId = this.GetPartitionId(instanceId);
            var state = await Client.GetOrchestrationStateAsync(partitionId, instanceId);

            if (state != null &&
                (executionId == null || executionId == state.OrchestrationInstance.ExecutionId))
            {
                return state;
            }
            else
            {
                return null;
            }
        }

        async Task<IList<OrchestrationState>> IOrchestrationServiceClient.GetOrchestrationStateAsync(
            string instanceId, 
            bool allExecutions)
        {
            var partitionId = this.GetPartitionId(instanceId);
            var state = await Client.GetOrchestrationStateAsync(partitionId, instanceId);

            if (state != null)
            {
                return new[] { state };
            }
            else
            {
                return new OrchestrationState[0];
            }
        }

        Task IOrchestrationServiceClient.ForceTerminateTaskOrchestrationAsync(
            string instanceId, 
            string message)
        {
            var partitionId = this.GetPartitionId(instanceId);
            return this.Client.ForceTerminateTaskOrchestrationAsync(partitionId, instanceId, message);
        }

        Task<string> IOrchestrationServiceClient.GetOrchestrationHistoryAsync(
            string instanceId, 
            string executionId)
        {
            throw new NotSupportedException(); //TODO
        }

        Task IOrchestrationServiceClient.PurgeOrchestrationHistoryAsync(
            DateTime thresholdDateTimeUtc, 
            OrchestrationStateTimeRangeFilterType 
            timeRangeFilterType)
        {
            throw new NotSupportedException(); //TODO
        }

        /******************************/
        // Task orchestration methods
        /******************************/

        Task<TaskOrchestrationWorkItem> IOrchestrationService.LockNextTaskOrchestrationWorkItemAsync(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            return this.OrchestrationWorkItemQueue.GetNext(receiveTimeout, cancellationToken);
        }

        Task IOrchestrationService.CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState,
            IList<TaskMessage> outboundMessages,
            IList<TaskMessage> orchestratorMessages,
            IList<TaskMessage> workItemTimerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState state)
        {
            var orchestrationWorkItem = (OrchestrationWorkItem)workItem;

            var partition = orchestrationWorkItem.Partition;
            partition.TraceContext.Value = "OWorker";

            partition.Submit(new BatchProcessed()
            {
                PartitionId = orchestrationWorkItem.Partition.PartitionId,
                SessionId = orchestrationWorkItem.SessionId,
                InstanceId = workItem.InstanceId,
                BatchStartPosition = orchestrationWorkItem.BatchStartPosition,
                BatchLength = orchestrationWorkItem.BatchLength,
                NewEvents = (List<HistoryEvent>)newOrchestrationRuntimeState.NewEvents,
                State = state,
                ActivityMessages = (List<TaskMessage>)outboundMessages,
                OrchestratorMessages = (List<TaskMessage>)orchestratorMessages,
                TimerMessages = (List<TaskMessage>)workItemTimerMessages,
                Timestamp = DateTime.UtcNow,
            });

            return completedTask;
        }

        Task IOrchestrationService.AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            // put it back into the work queue
            this.OrchestrationWorkItemQueue.Add(workItem);

            return completedTask;
        }

        Task IOrchestrationService.ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            return completedTask;
        }

        Task IOrchestrationService.RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            // no renewal required. Work items never time out.
            return Task.FromResult(workItem);
        }

        BehaviorOnContinueAsNew IOrchestrationService.EventBehaviourForContinueAsNew => BehaviorOnContinueAsNew.Carryover;

        bool IOrchestrationService.IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            return false;
        }

        int IOrchestrationService.GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            return 0;
        }

        int IOrchestrationService.GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            return 0;
        }

        int IOrchestrationService.MaxConcurrentTaskOrchestrationWorkItems => MaxConcurrentWorkItems;

        int IOrchestrationService.TaskOrchestrationDispatcherCount => 1;


        /******************************/
        // Task activity methods
        /******************************/

        Task<TaskActivityWorkItem> IOrchestrationService.LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            return this.ActivityWorkItemQueue.GetNext(receiveTimeout, cancellationToken);
        }

        Task IOrchestrationService.AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            // put it back into the work queue
            this.ActivityWorkItemQueue.Add(workItem);
            return completedTask;
        }

        Task IOrchestrationService.CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
        {
            var activityWorkItem = (ActivityWorkItem)workItem;
            var partition = activityWorkItem.Partition;
            partition.TraceContext.Value = "AWorker";
            partition.Submit(new ActivityCompleted()
            {
                PartitionId = activityWorkItem.Partition.PartitionId,
                ActivityId = activityWorkItem.ActivityId,
                Response = responseMessage,
            });
            return completedTask;
        }
        
        Task<TaskActivityWorkItem> IOrchestrationService.RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            // no renewal required. Work items never time out.
            return Task.FromResult(workItem);
        }

        int IOrchestrationService.MaxConcurrentTaskActivityWorkItems => MaxConcurrentWorkItems;

        int IOrchestrationService.TaskActivityDispatcherCount => 1;

    }
}