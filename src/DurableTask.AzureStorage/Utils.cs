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
    }
}
