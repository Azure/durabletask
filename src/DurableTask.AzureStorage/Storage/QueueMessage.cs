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
            this.Id = this.CloudQueueMessage.Id;
        }

        public QueueMessage(string message)
        {
            this.CloudQueueMessage = new CloudQueueMessage(message);
            this.Id = this.CloudQueueMessage.Id;
        }

        public CloudQueueMessage CloudQueueMessage { get; }

        public string Message {
            get
            {
                // Obtaining the contents of a queueMessage can yield de-serializations exceptions.
                // For example, if the message was encoded with a different encoding than the one used to decode it, this will throw.
                // There are many cases where we don't actually need the message contents, such as when we're just trying to peek at the message's Age
                // in the ScaleController. Therefore, we delay the decoding of the message until it's actually needed.
                return this.CloudQueueMessage.AsString;
            }
        }

        public string Id { get; }

        public int DequeueCount => this.CloudQueueMessage.DequeueCount;

        public DateTimeOffset? InsertionTime => this.CloudQueueMessage.InsertionTime;

        public DateTimeOffset? NextVisibleTime => this.CloudQueueMessage.NextVisibleTime;

        public string PopReceipt => this.CloudQueueMessage.PopReceipt;
    }
}
