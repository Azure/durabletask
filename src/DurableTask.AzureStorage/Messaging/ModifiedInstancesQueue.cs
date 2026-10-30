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
namespace DurableTask.AzureStorage.Messaging
{
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Storage;

    /// <summary>
    /// Encapsulates the Azure Storage queues used to record instances whose data has been modified after a
    /// migration away from the Azure Storage backend has started. Each message carries the instance ID and the
    /// sequence number that the source write is expected to produce.
    /// </summary>
    class ModifiedInstancesQueue
    {
        readonly Queue[] queues;

        public ModifiedInstancesQueue(AzureStorageClient azureStorageClient)
        {
            this.queues = new Queue[azureStorageClient.Settings.PartitionCount];
            for (int partitionIndex = 0; partitionIndex < this.queues.Length; partitionIndex++)
            {
                string queueName = AzureStorageOrchestrationService.GetModifiedInstancesQueueName(
                    azureStorageClient.Settings.TaskHubName, partitionIndex);
                this.queues[partitionIndex] = azureStorageClient.GetQueueReference(queueName);
            }
        }

        /// <summary>
        /// Creates the modified-instances queues if they do not already exist.
        /// </summary>
        public Task CreateIfNotExistsAsync(CancellationToken cancellationToken = default)
        {
            return Task.WhenAll(this.queues.Select(queue => queue.CreateIfNotExistsAsync(cancellationToken)));
        }

        /// <summary>
        /// Adds the specified instance ID and sequence number to the modified-instances queue.
        /// </summary>
        public Task AddInstanceAsync(
            string instanceId,
            long sequenceNumber,
            CancellationToken cancellationToken = default)
        {
            string message = Utils.SerializeToJson(new ModifiedInstanceMessage
            {
                InstanceId = instanceId,
                SequenceNumber = sequenceNumber,
            });

            // Use the same partition as the instance's control queue.
            uint partitionIndex = Fnv1aHashHelper.ComputeHash(instanceId) % (uint)this.queues.Length;
            return this.queues[partitionIndex].AddMessageAsync(message, visibilityDelay: null, cancellationToken: cancellationToken);
        }
    }

    class ModifiedInstanceMessage
    {
        public string InstanceId { get; set; } = string.Empty;

        public long SequenceNumber { get; set; }
    }
}
