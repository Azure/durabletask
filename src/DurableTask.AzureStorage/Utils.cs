//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace DurableTask.AzureStorage
{
    using System.Text;
    using Microsoft.WindowsAzure.Storage.Queue;
    using Newtonsoft.Json;

    static class Utils
    {
        static JsonSerializerSettings TaskMessageSerializerSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.Objects
        };

        public static string SerializeMessageData(MessageData messageData)
        {
            string rawContent = JsonConvert.SerializeObject(messageData, TaskMessageSerializerSettings);
            return rawContent;
        }

        public static MessageData DeserializeQueueMessage(CloudQueueMessage queueMessage)
        {
            MessageData envelope = JsonConvert.DeserializeObject<MessageData>(
                queueMessage.AsString,
                TaskMessageSerializerSettings);

            envelope.OriginalQueueMessage = queueMessage;
            envelope.TotalMessageSizeBytes = Encoding.UTF8.GetByteCount(queueMessage.AsString);

            return envelope;
        }

        public static string GetPartitionKeyForInstance(string instanceId, string executionId)
        {
            return instanceId + "_" + executionId;
        }
    }
}
