//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace DurableTask.AzureStorage
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using DurableTask;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Queue;

    /// <summary>
    /// Context associated with action triggered by a message in an Azure storage queue.
    /// </summary>
    class ReceivedMessageContext
    {
        static readonly ConcurrentDictionary<object, ReceivedMessageContext> ObjectAssociations;
        static readonly string UserAgent;

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

        ReceivedMessageContext(Guid traceActivityId, MessageData message)
        {
            this.traceActivityId = traceActivityId;
            this.messageData = message;

            this.storageOperationContext = new OperationContext();
            this.storageOperationContext.ClientRequestID = traceActivityId.ToString();
            this.storageOperationContext.SendingRequest += OnSendingRequest;
        }

        ReceivedMessageContext(Guid traceActivityId, List<MessageData> messageBatch)
        {
            this.traceActivityId = traceActivityId;
            this.messageDataBatch = messageBatch;

            this.storageOperationContext = new OperationContext();
            this.storageOperationContext.ClientRequestID = traceActivityId.ToString();
            this.storageOperationContext.SendingRequest += OnSendingRequest;
        }

        public MessageData MessageData
        {
            get { return this.messageData; }
        }

        public IReadOnlyList<MessageData> MessageDataBatch
        {
            get { return this.messageDataBatch; }
        }

        public string InstanceId
        {
            get
            {
                if (this.messageData != null)
                {
                    return this.messageData.TaskMessage.OrchestrationInstance.InstanceId;
                }
                else
                {
                    return this.messageDataBatch[0].TaskMessage.OrchestrationInstance.InstanceId;
                }
            }
        }

        public OperationContext StorageOperationContext
        {
            get { return this.storageOperationContext; }
        }

        public static ReceivedMessageContext CreateFromReceivedMessageBatch(List<MessageData> batch)
        {
            Guid newTraceActivityId = StartNewLogicalTraceScope();
            batch.ForEach(TraceMessageReceived);

            return new ReceivedMessageContext(newTraceActivityId, batch);
        }

        public static ReceivedMessageContext CreateFromReceivedMessage(CloudQueueMessage queueMessage)
        {
            MessageData data = Utils.DeserializeQueueMessage(queueMessage);

            Guid newTraceActivityId = StartNewLogicalTraceScope();
            TraceMessageReceived(data);

            return new ReceivedMessageContext(newTraceActivityId, data);
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

        static void TraceMessageReceived(MessageData data)
        {
            Guid newTraceActivityId = Guid.NewGuid();
            AnalyticsEventSource.SetLogicalTraceActivityId(newTraceActivityId);

            TaskMessage taskMessage = data.TaskMessage;
            CloudQueueMessage queueMessage = data.OriginalQueueMessage;

            AnalyticsEventSource.Log.ReceivedMessage(
                data.ActivityId,
                taskMessage.Event.EventType.ToString(),
                taskMessage.OrchestrationInstance.InstanceId,
                taskMessage.OrchestrationInstance.ExecutionId,
                queueMessage.Id,
                Math.Max(0, (int)DateTimeOffset.UtcNow.Subtract(queueMessage.InsertionTime.Value).TotalMilliseconds),
                queueMessage.DequeueCount,
                data.TotalMessageSizeBytes);
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

        public CloudQueueMessage CreateOutboundQueueMessage(TaskMessage taskMessage)
        {
            return CreateOutboundQueueMessageInternal(taskMessage);
        }

        internal static CloudQueueMessage CreateOutboundQueueMessageInternal(TaskMessage taskMessage)
        {
            // We transfer to a new trace activity ID every time a new outbound queue message is created.
            Guid outboundTraceActivityId = Guid.NewGuid();

            var data = new MessageData(taskMessage, outboundTraceActivityId);
            string rawContent = Utils.SerializeMessageData(data);

            AnalyticsEventSource.Log.SendingMessage(
                outboundTraceActivityId,
                taskMessage.Event.EventType.ToString(),
                taskMessage.OrchestrationInstance.InstanceId,
                taskMessage.OrchestrationInstance.ExecutionId,
                Encoding.UTF8.GetByteCount(rawContent));

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

        internal static void OnSendingRequest(object sender, RequestEventArgs e)
        {
            e.Request.UserAgent = UserAgent;
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
