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
#nullable enable
namespace DurableTask.AzureStorage.Storage
{
    using System;
    using Microsoft.WindowsAzure.Storage.Queue;

    class QueueMessage
    {
        public QueueMessage(CloudQueueMessage cloudQueueMessage)
        {
            this.CloudQueueMessage = cloudQueueMessage;
            try
            {
                this.Message = this.CloudQueueMessage.AsString;
            }
            catch (FormatException)
            {
                // This try-catch block ensures forwards compatibility with DTFx.AzureStorage v2.x, which does not guarantee base64 encoding of messages (messages not encoded at all).
                // Therefore, if we try to decode those messages as base64, we will have a format exception that will yield a poison message
                // RawString is an internal property of CloudQueueMessage, so we need to obtain it via reflection.
                System.Reflection.PropertyInfo rawStringProperty = typeof(CloudQueueMessage).GetProperty("RawString", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
                this.Message = (string)rawStringProperty.GetValue(this.CloudQueueMessage);
            }
            this.Id = this.CloudQueueMessage.Id;
        }

        public QueueMessage(string message)
        {
            this.CloudQueueMessage = new CloudQueueMessage(message);
            this.Message = this.CloudQueueMessage.AsString;
            this.Id = this.CloudQueueMessage.Id;
        }

        public CloudQueueMessage CloudQueueMessage { get; }

        public string Message { get; }

        public string Id { get; }

        public int DequeueCount => this.CloudQueueMessage.DequeueCount;

        public DateTimeOffset? InsertionTime => this.CloudQueueMessage.InsertionTime;

        public DateTimeOffset? NextVisibleTime => this.CloudQueueMessage.NextVisibleTime;

        public string PopReceipt => this.CloudQueueMessage.PopReceipt;
    }
}
