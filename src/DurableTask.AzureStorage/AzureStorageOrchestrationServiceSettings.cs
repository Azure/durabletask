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

namespace DurableTask.AzureStorage
{
    using System;
    using DurableTask.AzureStorage.Partitioning;
    using DurableTask.AzureStorage.Logging;
    using DurableTask.Core;
    using Microsoft.Extensions.Logging;
    using Microsoft.WindowsAzure.Storage.Queue;
    using Microsoft.WindowsAzure.Storage.Table;
    using System.Runtime.Serialization;

    /// <summary>
    /// Settings that impact the runtime behavior of the <see cref="AzureStorageOrchestrationService"/>.
    /// </summary>
    public class AzureStorageOrchestrationServiceSettings
    {
        internal const int DefaultPartitionCount = 4;

        internal static readonly TimeSpan DefaultMaxQueuePollingInterval = TimeSpan.FromSeconds(30);

        LogHelper logHelper;

        /// <summary>
        /// Gets or sets the name of the app.
        /// </summary>
        public string AppName { get; set; } = Utils.AppName;

        /// <summary>
        /// Gets or sets the number of messages to pull from the control queue at a time. The default is 32.
        /// The maximum batch size supported by Azure Storage Queues is 32.
        /// </summary>
        public int ControlQueueBatchSize { get; set; } = 32;

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
        /// Gets or sets a flag indicating whether to automatically fetch large orchestration input and outputs
        /// when it is stored in a compressed blob when retrieving orchestration state.
        /// </summary>
        public bool FetchLargeMessageDataEnabled { get; set; } = true;

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
        /// If true, takes a lease on the task hub container, allowing for only one app to process messages in a task hub at a time.
        /// </summary>
        public bool UseAppLease { get; set; } = true;

        /// <summary>
        /// If UseAppLease is true, gets or sets the AppLeaaseOptions used for acquiring the lease to start the application.
        /// </summary>
        public AppLeaseOptions AppLeaseOptions { get; set; } = AppLeaseOptions.DefaultOptions;

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
        /// When true, will throw an exception when attempting to create an orchestration with an existing dedupe status.
        /// </summary>
        public bool ThrowExceptionOnInvalidDedupeStatus { get; set; } = false;

        /// <summary>
        /// Use the legacy partition management strategy, which improves performance at the cost of being less resiliant
        /// to split brain.
        /// </summary>
        public bool UseLegacyPartitionManagement { get; set; } = false;

        /// <summary>
        /// User serialization that will respect <see cref="IExtensibleDataObject"/>. Default is false.
        /// </summary>
        public bool UseDataContractSerialization { get; set; }

        /// <summary>
        /// Gets or sets the optional <see cref="ILoggerFactory"/> to use for diagnostic logging.
        /// </summary>
        public ILoggerFactory LoggerFactory { get; set; } = NoOpLoggerFactory.Instance;

        /// <summary>
        /// Returns bool indicating is the TrackingStoreStorageAccount has been set.
        /// </summary>
        public bool HasTrackingStoreStorageAccount => this.TrackingStoreStorageAccountDetails != null;

        internal string HistoryTableName => this.HasTrackingStoreStorageAccount ? $"{this.TrackingStoreNamePrefix}History" : $"{this.TaskHubName}History";

        internal string InstanceTableName => this.HasTrackingStoreStorageAccount ? $"{this.TrackingStoreNamePrefix}Instances" : $"{this.TaskHubName}Instances";

        /// <summary>
        /// Gets an instance of <see cref="LogHelper"/> that can be used for writing structured logs.
        /// </summary>
        internal LogHelper Logger
        {
            get
            {
                if (this.logHelper == null)
                {
                    this.logHelper = new LogHelper(this.LoggerFactory.CreateLogger("DurableTask.AzureStorage"));
                }

                return this.logHelper;
            }
        }
    }
}
