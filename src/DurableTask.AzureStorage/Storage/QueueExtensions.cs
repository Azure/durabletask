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
using System.Threading;
using System;
using System.Threading.Tasks;
using Azure.Storage.Queues.Models;

namespace DurableTask.AzureStorage.Storage
{
    static class QueueExtensions
    {
        public static async Task UpdateMessageAsync(this Queue queue, MessageData messageData, TimeSpan visibilityTimeout, Guid? clientRequestId = null, CancellationToken cancellationToken = default)
        {
            if (queue == null)
            {
                throw new ArgumentNullException(nameof(queue));
            }

            if (messageData == null)
            {
                throw new ArgumentNullException(nameof(messageData));
            }

            UpdateReceipt receipt = await queue.UpdateMessageAsync(messageData.OriginalQueueMessage, visibilityTimeout, clientRequestId, cancellationToken);
            messageData.Update(receipt);
        }
    }
}
