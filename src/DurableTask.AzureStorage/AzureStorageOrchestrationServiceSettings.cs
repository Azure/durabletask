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
        /// <summary>
        /// Gets or sets the number of messages to pull from the control queue at a time. The default is 32.
        /// The maximum batch size supported by Azure Storage Queues is 32.
        /// </summary>
        public int ControlQueueBatchSize { get; set; } = 32;

        /// <summary>
        /// Gets or sets the visibility timeout of dequeued control queue messages. The default is 90 seconds.
        /// </summary>
        public TimeSpan ControlQueueVisibilityTimeout { get; set; } = TimeSpan.FromSeconds(90);

        /// <summary>
        /// Gets or sets the <see cref="QueueRequestOptions"/> that are provided to all internal 
        /// usage of <see cref="CloudQueue"/> APIs for the control queue.
        /// </summary>
        public QueueRequestOptions ControlQueueRequestOptions { get; set; }

        /// <summary>
        /// /// Gets or sets the visibility timeout of dequeued work item queue messages. The default is 90 seconds.
        /// </summary>
        public TimeSpan WorkItemQueueVisibilityTimeout { get; set; } = TimeSpan.FromSeconds(90);

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
    }
}
