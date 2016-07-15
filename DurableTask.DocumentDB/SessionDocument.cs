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

using DurableTask.History;
using Microsoft.Azure.Documents;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace DurableTask.DocumentDb
{
    // AFFANDAR : TODO : do we really need to dervice from document? how do we get the self link
    public class SessionDocument
    {
        [JsonProperty(PropertyName = "id")]
        public string InstanceId { get; set; }

        [JsonProperty(PropertyName = "executionId")]
        public string ExecutionId { get; set; }

        [JsonProperty(PropertyName = "executionHistory")]
        public IList<HistoryEvent> ExecutionHistory;

        [JsonProperty(PropertyName = "sessionLock")]
        public SessionLock SessionLock { get; set; }

        [JsonProperty(PropertyName = "state")]
        public OrchestrationState State { get; set; }

        [JsonProperty(PropertyName = "orchestratorQueueLastUpdatedTimeUtc")]
        public DateTime OrchestrationQueueLastUpdatedTimeUtc { get; set; }

        [JsonProperty(PropertyName="orchestrationQueue")]
        public IList<TaskMessage> OrchestrationQueue { get; set; }

        [JsonProperty(PropertyName = "activityQueueLastUpdatedTimeUtc")]
        public DateTime ActivityQueueLastUpdatedTimeUtc { get; set; }

        [JsonProperty(PropertyName = "activityQueue")]
        public IList<TaskMessage> ActivityQueue { get; set; }
    }

    // AFFANDAR : TODO : separate file
    public class SessionLock
    {
        public string LockToken { get; set; }
        public DateTime LockedUntilUtc { get; set;  }
    }
}