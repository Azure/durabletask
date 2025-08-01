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
#nullable enable
namespace DurableTask.AzureStorage
{
    using System;
    using System.Runtime.Serialization;
    using Azure.Data.Tables;
    using DurableTask.AzureStorage.Logging;
    using DurableTask.AzureStorage.Partitioning;
    using DurableTask.Core;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Settings that impact the runtime behavior of the <see cref="AzureStorageOrchestrationService"/>.
    /// </summary>
    public class AzureStorageOrchestrationServiceSettings
    {
        internal const int DefaultPartitionCount = 4;

        internal static readonly TimeSpan DefaultMaxQueuePollingInterval = TimeSpan.FromSeconds(30);

        LogHelper? logHelper;

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
        /// Gets or sets the visibility timeout of dequeued work item queue messages. The default is 5 minutes.
        /// </summary>
        public TimeSpan WorkItemQueueVisibilityTimeout { get; set; } = TimeSpan.FromMinutes(5);

        /// <summary>
        /// Gets or sets the prefix of the TrackingStore table name.
        /// </summary>
        /// <remarks>
        /// This property is only used when the value of the property <see cref="TrackingServiceClientProvider"/>
        /// is not <see langword="null"/>. The default is <c>"DurableTask"</c>.
        /// </remarks>
        public string TrackingStoreNamePrefix { get; set; } = "DurableTask";

        /// <summary>
        /// Gets or sets the name of the task hub. This value is used to group related storage resources.
        /// </summary>
        public string TaskHubName { get; set; } = "Default";

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
        /// Gets or sets the maximum number of entity operation batches that can be processed concurrently on a single node.
        /// The default value is 100.
        /// </summary>
        public int MaxConcurrentTaskEntityWorkItems { get; set; } = 100;

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
        /// Interval for which the lease is taken on Azure Blob representing a task hub partition in partition manager V1 (legacy partition manager) and V2 (safe partition manager).  
        /// The amount of time that a lease expiration deadline is extended on a renewal in partition manager V3 (table partition manager).
        /// If the lease is not renewed within this within this timespan, it will expire and ownership of the partition may move to another worker.
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
        /// When false, when an orchestrator is in a terminal state (e.g. Completed, Failed, Terminated), events for that orchestrator are discarded.
        /// Otherwise, events for a terminal orchestrator induce a replay. This may be used to recompute the state of the orchestrator in the "Instances Table".
        /// </summary>
        /// <remarks>
        /// Transactions across Azure Tables are not possible, so we independently update the "History table" and then the "Instances table"
        /// to set the state of the orchestrator.
        /// If a crash were to occur between these two updates, the state of the orchestrator in the "Instances table" would be incorrect.
        /// By setting this configuration to true, you can recover from these inconsistencies by forcing a replay of the orchestrator in response
        /// to a client event like a termination request or an external event, which gives the framework another opportunity to update the state of
        /// the orchestrator in the "Instances table". To force a replay after enabling this configuration, just send any external event to the affected instanceId.
        /// </remarks>
        public bool AllowReplayingTerminalInstances { get; set; } = false;

        /// <summary>
        /// Specifies the timeout (in seconds) for read and write operations on the partition table in partition manager V3 (table partition manager).
        /// This helps detect and recover from potential silent hangs caused by Azure Storage client's internal retries. 
        /// If the operation exceeds the timeout, a PartitionManagerWarning is logged and the operation is retried.
        /// The default time is 2 seconds.
        /// </summary>
        public TimeSpan PartitionTableOperationTimeout { get; set; } = TimeSpan.FromSeconds(2);

        /// <summary>
        /// If UseAppLease is true, gets or sets the AppLeaseOptions used for acquiring the lease to start the application.
        /// </summary>
        public AppLeaseOptions AppLeaseOptions { get; set; } = AppLeaseOptions.DefaultOptions;

        /// <summary>
        /// Gets or sets the client provider for the Azure Storage Account services used by the Durable Task Framework.
        /// </summary>
        public StorageAccountClientProvider? StorageAccountClientProvider { get; set; }

        /// <summary>
        /// Gets or sets the optional client provider for the Tracking Store that records the progress of orchestrations and entities.
        /// </summary>
        /// <remarks>
        /// If the value is <see langword="null"/>, then the <see cref="StorageAccountClientProvider"/> is used instead.
        /// </remarks>
        public TrackingServiceClientProvider? TrackingServiceClientProvider { get; set; }

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
        /// Use the newer Azure Tables-based partition manager instead of the older Azure Blobs-based partition manager. The default value is <c>false</c>.
        /// </summary>
        public bool UseTablePartitionManagement { get; set; } = true;

        /// <summary>
        /// User serialization that will respect <see cref="IExtensibleDataObject"/>. Default is false.
        /// </summary>
        public bool UseDataContractSerialization { get; set; }

        /// <summary>
        /// Gets or sets the optional <see cref="ILoggerFactory"/> to use for diagnostic logging.
        /// </summary>
        public ILoggerFactory LoggerFactory { get; set; } = NoOpLoggerFactory.Instance;

        /// <summary>
        /// Gets or sets a value indicating whether to disable the ExecutionStarted de-duplication logic.
        /// </summary>
        public bool DisableExecutionStartedDeduplication { get; set; }

        /// <summary>
        /// Gets or sets an optional custom type binder used when trying to deserialize queued messages.
        /// </summary>
        public ICustomTypeBinder? CustomMessageTypeBinder { get; set; }

        /// <summary>
        /// Returns bool indicating is the <see cref="TrackingServiceClientProvider"/> has been set.
        /// </summary>
        public bool HasTrackingStoreStorageAccount => this.TrackingServiceClientProvider != null;

        internal string HistoryTableName => this.HasTrackingStoreStorageAccount ? $"{this.TrackingStoreNamePrefix}History" : $"{this.TaskHubName}History";

        internal string InstanceTableName => this.HasTrackingStoreStorageAccount ? $"{this.TrackingStoreNamePrefix}Instances" : $"{this.TaskHubName}Instances";
        internal string PartitionTableName => $"{this.TaskHubName}Partitions";

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

        /// <summary>
        /// Gets or sets the limit on the number of entity operations that should be processed as a single batch.
        /// A null value indicates that no particular limit should be enforced.
        /// </summary>
        /// <remarks>
        /// Limiting the batch size can help to avoid timeouts in execution environments that impose time limitations on work items.
        /// If set to 1, batching is disabled, and each operation executes as a separate work item.
        /// </remarks>
        /// <value>
        /// A positive integer, or null.
        /// </value>
        public int? MaxEntityOperationBatchSize { get; set; } = null;

        /// <summary>
        /// Gets or sets the time window within which entity messages get deduplicated and reordered.
        /// If set to zero, there is no sorting or deduplication, and all messages are just passed through.
        /// </summary>
        public int EntityMessageReorderWindowInMinutes { get; set; } = 30;

        /// <summary>
        /// Whether to use separate work item queues for entities and orchestrators.
        /// This defaults to false, to avoid issues when using this provider from code that does not support separate dispatch.
        /// Consumers that require separate dispatch (such as the new out-of-proc v2 SDKs) must set this to true.
        /// </summary>
        public bool UseSeparateQueueForEntityWorkItems { get; set; } = false;

        /// <summary>
        /// Gets or sets the encoding strategy used for Azure Storage Queue messages.
        /// The default is <see cref="QueueClientMessageEncoding.UTF8"/>.
        /// </summary>
        public QueueClientMessageEncoding QueueClientMessageEncoding { get; set; } = QueueClientMessageEncoding.UTF8;
    }
}
