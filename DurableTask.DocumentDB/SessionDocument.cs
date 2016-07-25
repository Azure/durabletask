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
using Newtonsoft.Json.Converters;

namespace DurableTask.DocumentDb
{
    // AFFANDAR : TODO : do we really need to dervice from document? how do we get the self link
    public class SessionDocument
    {
        [JsonProperty(PropertyName = "id")]
        public string InstanceId { get; set; }

        [JsonProperty(PropertyName = "_etag")]
        public string Etag { get; set; }

        [JsonProperty(PropertyName = "executionId")]
        public string ExecutionId { get; set; }

        [JsonProperty(PropertyName = "orchestrationRuntimeState")]
        public OrchestrationRuntimeState OrchestrationRuntimeState;

        [JsonProperty(PropertyName = "sessionLock")]
        public SessionLock SessionLock { get; set; }

        [JsonProperty(PropertyName = "state")]
        public OrchestrationState State { get; set; }

        [JsonProperty(PropertyName = "orchestratorQueueLastUpdatedTimeUtc")]
        public DateTime OrchestrationQueueLastUpdatedTimeUtc { get; set; }

        [JsonProperty(PropertyName="orchestrationQueue")]
        public IList<TaskMessageDocument> OrchestrationQueue { get; set; }

        [JsonProperty(PropertyName = "activityQueueLastUpdatedTimeUtc")]
        public DateTime ActivityQueueLastUpdatedTimeUtc { get; set; }

        [JsonProperty(PropertyName = "activityQueue")]
        public IList<TaskMessageDocument> ActivityQueue { get; set; }

        [JsonConverter(typeof(StringEnumConverter))]
        [JsonProperty(PropertyName = "documentType")]
        public DocumentType DocumentType { get; set; }

        public SessionDocument()
        {
            this.DocumentType = DocumentType.SessionDocument;
        }
    }

    // AFFANDAR : TODO : we would need to separate out queues from main session doc for higher throughput scenarios
    //public class EventQueueItem
    //{
    //    public HistoryEvent Event;
    //    public int Sequence;
    //}

    public class TaskMessageDocument
    {
        [JsonProperty(PropertyName = "taskMessage")]
        public TaskMessage TaskMessage { get; set; }

        [JsonProperty(PropertyName = "messageId")]
        public Guid MessageId { get; set; }

        public TaskMessageDocument()
        {
            
        }

        public TaskMessageDocument(TaskMessage taskMessage, Guid messageId)
        {
            this.TaskMessage = taskMessage;
            this.MessageId = messageId;
        }
    }

    public enum DocumentType
    {
        SessionDocument,
        OrchestrationQueueItem,
        ActivityQueueItem,
    }

    public class SessionLock
    {
        public string LockToken { get; set; }
        public DateTime LockedUntilUtc { get; set;  }
    }
}