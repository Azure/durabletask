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
    using Azure;
    using Azure.Data.Tables;

    /// <summary>
    /// Table Entity Representation of an Orchestration Instance's Status
    /// </summary>
    class OrchestrationInstanceStatus : ITableEntity
    {
        public string ExecutionId { get; set; }
        public string Name { get; set; }
        public string Version { get; set; }
        public string Input { get; set; }
        public string Output { get; set; }
        public string CustomStatus { get; set; }
        public DateTime CreatedTime { get; set; }
        public DateTime LastUpdatedTime { get; set; }
        public DateTime? CompletedTime { get; set; }
        public string RuntimeStatus { get; set; }
        public DateTime? ScheduledStartTime { get; set; }
        public int Generation { get; set; }
        public string Tags { get; set; }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }
    }
}
