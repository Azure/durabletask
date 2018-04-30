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

namespace DurableTask.AzureStorage.Partitioning
{
    using System;

    class TaskHubInfo
    {
        public TaskHubInfo(string TaskHubName, DateTime createdAt, int partitionCount)
        {
            this.TaskHubName = TaskHubName;
            this.CreatedAt = createdAt;
            this.PartitionCount = partitionCount;
        }

        public string TaskHubName { get; private set; }
        public DateTime CreatedAt { get; private set; }
        public int PartitionCount { get; private set; }
    }
}
