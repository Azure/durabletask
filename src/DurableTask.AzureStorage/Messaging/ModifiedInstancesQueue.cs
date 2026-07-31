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
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Storage;

    /// <summary>
    /// Encapsulates the Azure Storage queue used to record instances whose data has been modified after a live
    /// migration away from the Azure Storage backend has started. Each message is a JSON <see cref="ModifiedInstanceMessage"/>
    /// carrying the instance ID and execution ID; the migration process drains this queue to move the latest state of
    /// each modified instance to the target backend.
    /// </summary>
    class ModifiedInstancesQueue
    {
        readonly Queue queue;

        public ModifiedInstancesQueue(AzureStorageClient azureStorageClient)
        {
            string queueName = AzureStorageOrchestrationService.GetQueueName(azureStorageClient.Settings.TaskHubName, "modifiedinstances");
            this.queue = azureStorageClient.GetQueueReference(queueName);
        }

        public string Name => this.queue.Name;

        /// <summary>
        /// Creates the modified-instances queue if it does not already exist. This operation is idempotent.
        /// </summary>
        public Task CreateIfNotExistsAsync(CancellationToken cancellationToken = default)
        {
            return this.queue.CreateIfNotExistsAsync(cancellationToken);
        }

        /// <summary>
        /// Adds the specified instance ID and execution ID to the modified-instances queue.
        /// </summary>
        public Task AddInstanceAsync(string instanceId, string executionId, CancellationToken cancellationToken = default)
        {
            string message = Utils.SerializeToJson(new ModifiedInstanceMessage
            {
                InstanceId = instanceId,
                ExecutionId = executionId,
            });

            return this.queue.AddMessageAsync(message, visibilityDelay: null, cancellationToken: cancellationToken);
        }
    }

    /// <summary>
    /// The payload of a modified-instances queue message.
    /// </summary>
    class ModifiedInstanceMessage
    {
        public string? InstanceId { get; set; }

        public string? ExecutionId { get; set; }
    }
}
