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

namespace DurableTask.EventHubs
{
    using System;

    /// <summary>
    /// Settings for the <see cref="EventHubsOrchestrationService"/> class.
    /// </summary>
    public class EventHubsOrchestrationServiceSettings
    {
        /// <summary>
        /// Gets or sets the connection string for the event hubs namespace.
        /// </summary>
        public string EventHubsConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the name of the event hub.
        /// </summary>
        public string EventHubName { get; set; }

        /// <summary>
        /// Gets or sets the name of the task hub.
        /// </summary>
        public string TaskHubName { get; set; }

        /// <summary>
        /// Gets or sets the connection string for the Azure storage used for leases and checkpoints.
        /// </summary>
        public string StorageConnectionString { get; set; }

        /// <summary>
        /// Bypasses event hubs and uses in-memory emulation instead.
        /// </summary>
        public bool UseEmulatedBackend => (string.IsNullOrEmpty(this.EventHubsConnectionString));

        /// <summary>
        /// Gets or sets the number of partitions to use when creating an event hub.
        /// If the event hub already exists, this number is ignored.
        /// </summary>
        public int NumberPartitions { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of work items that can be processed concurrently on a single node.
        /// The default value is 100.
        /// </summary>
        public int MaxConcurrentTaskActivityWorkItems { get; set; } = 100;

        /// <summary>
        /// Gets or sets the maximum number of orchestrations that can be processed concurrently on a single node.
        /// The default value is 100.
        /// </summary>
        public int MaxConcurrentTaskOrchestrationWorkItems { get; set; } = 100;

        /// <summary>
        /// Validates the specified <see cref="EventHubsOrchestrationServiceSettings"/> object.
        /// </summary>
        /// <param name="settings">The <see cref="EventHubsOrchestrationServiceSettings"/> object to validate.</param>
        /// <returns>Returns <paramref name="settings"/> if successfully validated.</returns>
        public static EventHubsOrchestrationServiceSettings Validate(EventHubsOrchestrationServiceSettings settings)
        {
            if (settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }

            if (!settings.UseEmulatedBackend && string.IsNullOrEmpty(settings.EventHubsConnectionString))
            {
                throw new ArgumentNullException(nameof(settings.EventHubsConnectionString));
            }

            if (string.IsNullOrEmpty(settings.StorageConnectionString))
            {
                throw new ArgumentNullException(nameof(settings.StorageConnectionString));
            }

            if (settings.NumberPartitions < 1 || settings.NumberPartitions > 32)
            {
                throw new ArgumentOutOfRangeException(nameof(settings.NumberPartitions));
            }

            if (settings.MaxConcurrentTaskOrchestrationWorkItems <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(settings.MaxConcurrentTaskOrchestrationWorkItems));
            }

            if (settings.MaxConcurrentTaskActivityWorkItems <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(settings.MaxConcurrentTaskActivityWorkItems));
            }

            return settings;
        }

        /// <summary>
        /// Computes the partition for the given instance.
        /// </summary>
        /// <param name="instanceId">The instance id.</param>
        /// <returns>The partition id.</returns>
        public uint GetPartitionId(string instanceId)
        {
            return Fnv1aHashHelper.ComputeHash(instanceId) % (uint)this.NumberPartitions;
        }
    }
}
