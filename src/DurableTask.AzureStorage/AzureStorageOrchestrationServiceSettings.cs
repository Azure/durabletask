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

namespace DurableTask.AzureStorage
{
    using System;
    using Microsoft.WindowsAzure.Storage.Queue;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Settings that impact the runtime behavior of the <see cref="AzureStorageOrchestrationService"/>.
    /// </summary>
    public class AzureStorageOrchestrationServiceSettings
    {
        internal const int DefaultPartitionCount = 4;

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
    }
}
