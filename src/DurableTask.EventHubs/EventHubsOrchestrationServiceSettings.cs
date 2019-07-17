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
    using DurableTask.Core;
    using Microsoft.Azure.EventHubs;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Queue;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Settings that impact the runtime behavior of the <see cref="EventHubsOrchestrationService"/>.
    /// </summary>
    public class EventHubsOrchestrationServiceSettings
    {
        // TODO: Remove - this is controlled by Event Hubs
        internal const int DefaultPartitionCount = 4;

        internal static readonly TimeSpan DefaultMaxQueuePollingInterval = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Gets or sets the Azure Event Hubs connection string.
        /// </summary>
        public string EventHubConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the name of the Event Hubs consumer group.
        /// </summary>
        public string ConsumerGroupName { get; set; } = PartitionReceiver.DefaultConsumerGroupName;

        /// <summary>
        /// Gets or sets the maximum number of events to pull from the partition at a time.
        /// The default is 100.
        /// </summary>
        public int MaxReceiveMessageCount { get; set; } = 100;

        /// <summary>
        /// Gets or sets the number of control queue messages that can be buffered in memory at a time, at which
        /// point the dispatcher will wait before dequeuing any additional messages. The default is 64.
        /// </summary>
        public int ControlQueueBufferThreshold { get; set; } = 64;

        /// <summary>
        /// Gets or sets the visibility timeout of dequeued control queue messages. The default is 5 minutes.
        /// </summary>
        public TimeSpan ControlQueueVisibilityTimeout { get; set; } = TimeSpan.FromMinutes(5);

        /// <summary>
        /// Gets or sets the <see cref="QueueRequestOptions"/> that are provided to all internal 
        /// usage of <see cref="CloudQueue"/> APIs for the control queue.
        /// </summary>
        public QueueRequestOptions ControlQueueRequestOptions { get; set; }

        /// <summary>
        /// Gets or sets the visibility timeout of dequeued work item queue messages. The default is 5 minutes.
        /// </summary>
        public TimeSpan WorkItemQueueVisibilityTimeout { get; set; } = TimeSpan.FromMinutes(5);

        /// <summary>
        /// Gets or sets the <see cref="QueueRequestOptions"/> that are provided to all internal 
        /// usage of <see cref="CloudQueue"/> APIs for the work item queue.
        /// </summary>
        public QueueRequestOptions WorkItemQueueRequestOptions { get; set; }

        /// <summary>
        /// Gets or sets the <see cref="TableRequestOptions"/> that are provided to all internal
        /// usage of the <see cref="CloudTable"/> APIs for the history table.
        /// </summary>
        public TableRequestOptions HistoryTableRequestOptions { get; set; }

        /// <summary>
        /// Gets or sets the Azure Storage connection string.
        /// </summary>
        public string StorageConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the prefix of the TrackingStore table name.
        /// This property is only used when we have TrackingStoreStorageAccountDetails.
        /// The default is "DurableTask"
        /// </summary>
        public string TrackingStoreNamePrefix { get; set; } = "DurableTask";

        /// <summary>
        /// Gets or sets the name of the task hub. This value is used to group related storage resources.
        /// </summary>
        public string TaskHubName { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of work items that can be processed concurrently on a single node.
        /// The default value is 10.
        /// </summary>
        public int MaxConcurrentTaskActivityWorkItems { get; set; } = 10;

        /// <summary>
        /// Gets or sets the maximum number of orchestrations that can be processed concurrently on a single node.
        /// The default value is 100.
        /// </summary>
        public int MaxConcurrentTaskOrchestrationWorkItems { get; set; } = 100;

        /// <summary>
        /// Gets or sets the maximum number of concurrent storage operations that can be executed in the context
        /// of a single orchestration instance.
        /// </summary>
        public int MaxStorageOperationConcurrency { get; set; } = Environment.ProcessorCount * 25;

        /// <summary>
        /// Gets the maximum number of orchestrator actions to checkpoint at a time.
        /// </summary>
        public int MaxCheckpointBatchSize { get; set; }

        /// <summary>
        /// Gets or sets the identifier for the current worker.
        /// </summary>
        public string WorkerId { get; set; } = Environment.MachineName;

        /// <summary>
        /// Gets or sets the maximum number of orchestration partitions.
        /// </summary>
        public int PartitionCount { get; set; } = DefaultPartitionCount;

        /// <summary>
        /// Gets or sets a flag indicating whether to enable extended sessions.
        /// </summary>
        public bool ExtendedSessionsEnabled { get; set; }

        /// <summary>
        /// Gets or sets the number of seconds before an idle session times out.
        /// </summary>
        public TimeSpan ExtendedSessionIdleTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Renew interval for all leases for partitions currently held.
        /// </summary>
        public TimeSpan LeaseRenewInterval { get; set; } = TimeSpan.FromSeconds(10);

        /// <summary>
        /// Interval when the current worker instance kicks off a task to compute if partitions are distributed evenly.
        /// among known host instances. 
        /// </summary>
        public TimeSpan LeaseAcquireInterval { get; set; } = TimeSpan.FromSeconds(10);

        /// <summary>
        /// Interval for which the lease is taken on Azure Blob representing a task hub partition.  If the lease is not renewed within this 
        /// interval, it will cause it to expire and ownership of the partition will move to another worker instance.
        /// </summary>
        public TimeSpan LeaseInterval { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Maximum interval for polling control and work-item queues.
        /// </summary>
        public TimeSpan MaxQueuePollingInterval { get; set; } = DefaultMaxQueuePollingInterval;

        /// <summary>
        /// Gets or sets the Azure Storage Account details
        /// If provided, this is used to connect to Azure Storage
        /// </summary>
        public StorageAccountDetails StorageAccountDetails { get; set; }

        /// <summary>
        /// Gets or sets the Storage Account Details for Tracking Store.
        /// In case of null, StorageAccountDetails is applied. 
        /// </summary>
        public StorageAccountDetails TrackingStoreStorageAccountDetails { get; set; }
        
        /// <summary>
        ///  Should we carry over unexecuted raised events to the next iteration of an orchestration on ContinueAsNew
        /// </summary>
        public BehaviorOnContinueAsNew EventBehaviourForContinueAsNew { get; set; } = BehaviorOnContinueAsNew.Carryover;

        /// <summary>
        /// Returns bool indicating is the TrackingStoreStorageAccount has been set.
        /// </summary>
        public  bool HasTrackingStoreStorageAccount => TrackingStoreStorageAccountDetails != null;

        internal string HistoryTableName => this.HasTrackingStoreStorageAccount ? $"{this.TrackingStoreNamePrefix}History" : $"{this.TaskHubName}History";

        internal string InstanceTableName => this.HasTrackingStoreStorageAccount ? $"{this.TrackingStoreNamePrefix}Instances" : $"{this.TaskHubName}Instances";

        /// <summary>
        /// Validates the specified <see cref="EventHubsOrchestrationServiceSettings"/> object.
        /// </summary>
        /// <param name="settings">The <see cref="EventHubsOrchestrationServiceSettings"/> object to validate.</param>
        public static void Validate(EventHubsOrchestrationServiceSettings settings)
        {
            if (string.IsNullOrEmpty(settings.TaskHubName))
            {
                throw new ArgumentNullException(nameof(settings.TaskHubName));
            }

            if (string.IsNullOrEmpty(settings.EventHubConnectionString))
            {
                throw new ArgumentNullException(nameof(settings.EventHubConnectionString));
            }

            if (string.IsNullOrEmpty(settings.ConsumerGroupName))
            {
                throw new ArgumentNullException(nameof(settings.ConsumerGroupName));
            }

            if (settings.MaxReceiveMessageCount < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(settings.MaxReceiveMessageCount), "The maximum received message count must be 1 or greater.");
            }

            // TODO: More validation.
        }

        internal CloudStorageAccount GetStorageAccount()
        {
            CloudStorageAccount account;
            if (this.StorageAccountDetails != null)
            {
                account = this.StorageAccountDetails.ToCloudStorageAccount();
            }
            else
            {
                account = CloudStorageAccount.Parse(this.StorageConnectionString);
            }

            return account;
        }

        internal EventHubClient CreateEventHubClient()
        {
            var builder = new EventHubsConnectionStringBuilder(this.EventHubConnectionString);
            if (string.IsNullOrEmpty(builder.EntityPath))
            {
                // REVIEW: Should we throw an error if an EntityPath is already specified?
                builder.EntityPath = this.TaskHubName;
            }
            
            return EventHubClient.CreateFromConnectionString(builder.ToString());
        }
    }
}
