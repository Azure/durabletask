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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using DurableTask.Core;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Queue;

    /// <summary>
    /// Context associated with action triggered by a message in an Azure storage queue.
    /// </summary>
    class ReceivedMessageContext
    {
        static readonly ConcurrentDictionary<object, ReceivedMessageContext> ObjectAssociations;
        static readonly string UserAgent;

        readonly string storageAccountName;
        readonly string taskHub;
        readonly Guid traceActivityId;
        readonly MessageData messageData;
        readonly List<MessageData> messageDataBatch;
        readonly OperationContext storageOperationContext;

        static ReceivedMessageContext()
        {
            // All requests to storage will contain the assembly name and version in the User-Agent header.
            Assembly currentAssembly = typeof(ReceivedMessageContext).Assembly;
            var fileInfo = FileVersionInfo.GetVersionInfo(currentAssembly.Location);
            var assemblyFileVersion = new Version(
                fileInfo.FileMajorPart,
                fileInfo.FileMinorPart,
                fileInfo.FileBuildPart,
                fileInfo.FilePrivatePart);

            UserAgent = $"{currentAssembly.GetName().Name}/{assemblyFileVersion}";

            ObjectAssociations = new ConcurrentDictionary<object, ReceivedMessageContext>();
        }

        ReceivedMessageContext(string storageAccountName, string taskHub, Guid traceActivityId, MessageData message)
        {
            this.storageAccountName = storageAccountName ?? throw new ArgumentNullException(nameof(storageAccountName));
            this.taskHub = taskHub ?? throw new ArgumentNullException(nameof(taskHub));
            this.traceActivityId = traceActivityId;
            this.messageData = message ?? throw new ArgumentNullException(nameof(message));

            this.storageOperationContext = new OperationContext();
            this.storageOperationContext.ClientRequestID = traceActivityId.ToString();
        }

        ReceivedMessageContext(string storageAccountName, string taskHub, Guid traceActivityId, List<MessageData> messageBatch)
        {
            this.storageAccountName = storageAccountName ?? throw new ArgumentNullException(nameof(storageAccountName));
            this.taskHub = taskHub ?? throw new ArgumentNullException(nameof(taskHub));
            this.traceActivityId = traceActivityId;
            this.messageDataBatch = messageBatch ?? throw new ArgumentNullException(nameof(messageBatch));

            this.storageOperationContext = new OperationContext();
            this.storageOperationContext.ClientRequestID = traceActivityId.ToString();
        }

        public MessageData MessageData
        {
            get { return this.messageData; }
        }

        public IReadOnlyList<MessageData> MessageDataBatch
        {
            get { return this.messageDataBatch; }
        }

        public OrchestrationInstance Instance
        {
            get
            {
                if (this.messageData != null)
                {
                    return this.messageData.TaskMessage.OrchestrationInstance;
                }
                else
                {
                    return this.messageDataBatch[0].TaskMessage.OrchestrationInstance;
                }
            }
        }

        public OperationContext StorageOperationContext
        {
            get { return this.storageOperationContext; }
        }

        public static ReceivedMessageContext CreateFromReceivedMessageBatch(string storageAccountName, string taskHub, List<MessageData> batch)
        {
            if (batch == null)
            {
                throw new ArgumentNullException(nameof(batch));
            }

            if (batch.Count == 0)
            {
                throw new ArgumentException("The list must not be empty.", nameof(batch));
            }

            Guid newTraceActivityId = StartNewLogicalTraceScope();
            batch.ForEach(m => TraceMessageReceived(storageAccountName, taskHub, m));

            return new ReceivedMessageContext(storageAccountName, taskHub, newTraceActivityId, batch);
        }

        public static ReceivedMessageContext CreateFromReceivedMessage(
            string storageAccountName,
            string taskHub,
            CloudQueueMessage queueMessage,
            string queueName)
        {
            MessageData data = Utils.DeserializeQueueMessage(queueMessage, queueName);

            Guid newTraceActivityId = StartNewLogicalTraceScope();
            TraceMessageReceived(storageAccountName, taskHub, data);

            return new ReceivedMessageContext(storageAccountName, taskHub, newTraceActivityId, data);
        }

        static Guid StartNewLogicalTraceScope()
        {
            // This call sets the activity trace ID both on the current thread context
            // and on the logical call context. AnalyticsEventSource will use this 
            // activity ID for all trace operations.
            Guid newTraceActivityId = Guid.NewGuid();
            AnalyticsEventSource.SetLogicalTraceActivityId(newTraceActivityId);
            return newTraceActivityId;
        }

        static void TraceMessageReceived(string storageAccountName, string taskHub, MessageData data)
        {
            TaskMessage taskMessage = data.TaskMessage;
            CloudQueueMessage queueMessage = data.OriginalQueueMessage;

            AnalyticsEventSource.Log.ReceivedMessage(
                data.ActivityId,
                storageAccountName,
                taskHub,
                taskMessage.Event.EventType.ToString(),
                taskMessage.OrchestrationInstance.InstanceId,
                taskMessage.OrchestrationInstance.ExecutionId,
                queueMessage.Id,
                Math.Max(0, (int)DateTimeOffset.UtcNow.Subtract(queueMessage.InsertionTime.Value).TotalMilliseconds),
                queueMessage.DequeueCount,
                data.TotalMessageSizeBytes,
                PartitionId: data.QueueName);
        }

        public DateTime GetNextMessageExpirationTimeUtc()
        {
            if (this.messageData != null)
            {
                return this.messageData.OriginalQueueMessage.NextVisibleTime.Value.UtcDateTime;
            }
            else
            {
                return this.messageDataBatch.Min(msg => msg.OriginalQueueMessage.NextVisibleTime.Value.UtcDateTime);
            }
        }

        public CloudQueueMessage CreateOutboundQueueMessage(TaskMessage taskMessage, string queueName)
        {
            return CreateOutboundQueueMessageInternal(this.storageAccountName, this.taskHub, queueName, taskMessage);
        }

        internal static CloudQueueMessage CreateOutboundQueueMessageInternal(
            string storageAccountName,
            string taskHub,
            string queueName,
            TaskMessage taskMessage)
        {
            // We transfer to a new trace activity ID every time a new outbound queue message is created.
            Guid outboundTraceActivityId = Guid.NewGuid();

            var data = new MessageData(taskMessage, outboundTraceActivityId, queueName);
            string rawContent = Utils.SerializeMessageData(data);

            AnalyticsEventSource.Log.SendingMessage(
                outboundTraceActivityId,
                storageAccountName,
                taskHub,
                taskMessage.Event.EventType.ToString(),
                taskMessage.OrchestrationInstance.InstanceId,
                taskMessage.OrchestrationInstance.ExecutionId,
                Encoding.UTF8.GetByteCount(rawContent),
                PartitionId: data.QueueName);

            return new CloudQueueMessage(rawContent);
        }

        public bool TrySave(object relatedObject)
        {
            if (relatedObject == null)
            {
                throw new ArgumentNullException(nameof(relatedObject));
            }

            return ObjectAssociations.TryAdd(relatedObject, this);
        }

        public static bool TryRestoreContext(object relatedObject, out ReceivedMessageContext context)
        {
            if (!ObjectAssociations.TryGetValue(relatedObject, out context))
            {
                return false;
            }

            RestoreContext(context);
            return true;
        }

        public static bool RemoveContext(object relatedObject)
        {
            ReceivedMessageContext ignoredContext;
            return ObjectAssociations.TryRemove(relatedObject, out ignoredContext);
        }

        static void RestoreContext(ReceivedMessageContext context)
        {
            // This call sets the activity trace ID both on the current thread context
            // and on the logical call context. AnalyticsEventSource will use this 
            // activity ID for all trace operations.
            AnalyticsEventSource.SetLogicalTraceActivityId(context.traceActivityId);
        }
    }
}
