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

namespace DurableTask.AzureStorage.Messaging
{
    using System;
    using DurableTask.Core;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Queue;

    abstract class SessionBase
    {
        readonly string storageAccountName;
        readonly string taskHubName;
        readonly Guid traceActivityId;

        public SessionBase(string storageAccountName, string taskHubName, OrchestrationInstance orchestrationInstance, Guid traceActivityId)
        {
            this.storageAccountName = storageAccountName ?? throw new ArgumentNullException(nameof(storageAccountName));
            this.taskHubName = taskHubName ?? throw new ArgumentNullException(nameof(taskHubName));
            this.Instance = orchestrationInstance ?? throw new ArgumentNullException(nameof(orchestrationInstance));

            this.traceActivityId = traceActivityId;
            this.StorageOperationContext = new OperationContext
            {
                ClientRequestID = this.traceActivityId.ToString(),
            };
        }

        public OrchestrationInstance Instance { get; }

        public OperationContext StorageOperationContext { get; }

        public void StartNewLogicalTraceScope()
        {
            // This call sets the activity trace ID both on the current thread context
            // and on the logical call context. AnalyticsEventSource will use this 
            // activity ID for all trace operations.
            AnalyticsEventSource.SetLogicalTraceActivityId(this.traceActivityId);
        }

        public void TraceProcessingMessage(MessageData data, bool isExtendedSession)
        {
            if (data == null)
            {
                throw new ArgumentNullException(nameof(data));
            }

            TaskMessage taskMessage = data.TaskMessage;
            CloudQueueMessage queueMessage = data.OriginalQueueMessage;

            AnalyticsEventSource.Log.ProcessingMessage(
                data.ActivityId,
                this.storageAccountName,
                this.taskHubName,
                taskMessage.Event.EventType.ToString(),
                taskMessage.OrchestrationInstance.InstanceId,
                taskMessage.OrchestrationInstance.ExecutionId,
                queueMessage.Id,
                Math.Max(0, (int)DateTimeOffset.UtcNow.Subtract(queueMessage.InsertionTime.Value).TotalMilliseconds),
                isExtendedSession,
                Utils.ExtensionVersion);
        }
    }
}
