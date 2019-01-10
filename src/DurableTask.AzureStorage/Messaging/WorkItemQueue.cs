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
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Monitoring;
    using Microsoft.WindowsAzure.Storage.Queue;

    class WorkItemQueue : TaskHubQueue
    {
        public WorkItemQueue(
            CloudQueue storageQueue,
            AzureStorageOrchestrationServiceSettings settings,
            AzureStorageOrchestrationServiceStats stats,
            MessageManager messageManager)
            : base(storageQueue, settings, stats, messageManager)
        {
        }

        protected override QueueRequestOptions QueueRequestOptions => this.settings.WorkItemQueueRequestOptions;

        protected override TimeSpan MessageVisibilityTimeout => this.settings.WorkItemQueueVisibilityTimeout;

        public async Task<MessageData> GetMessageAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    CloudQueueMessage queueMessage = await this.storageQueue.GetMessageAsync(
                        this.settings.WorkItemQueueVisibilityTimeout,
                        this.settings.WorkItemQueueRequestOptions,
                        null /* operationContext */,
                        cancellationToken);

                    this.stats.StorageRequests.Increment();

                    if (queueMessage == null)
                    {
                        await this.backoffHelper.WaitAsync(cancellationToken);
                        continue;
                    }

                    this.stats.MessagesRead.Increment();

                    MessageData data = await this.messageManager.DeserializeQueueMessageAsync(
                        queueMessage,
                        this.storageQueue.Name);

                    this.backoffHelper.Reset();
                    return data;
                }
                catch (Exception e)
                {
                    if (!cancellationToken.IsCancellationRequested)
                    {
                        AnalyticsEventSource.Log.MessageFailure(
                            this.storageAccountName,
                            this.settings.TaskHubName,
                            string.Empty /* MessageId */,
                            string.Empty /* InstanceId */,
                            string.Empty /* ExecutionId */,
                            this.storageQueue.Name,
                            string.Empty /* EventType */,
                            0 /* TaskEventId */,
                            e.ToString(),
                            Utils.ExtensionVersion);

                        await this.backoffHelper.WaitAsync(cancellationToken);
                    }
                }
            }

            return null;
        }
    }
}
