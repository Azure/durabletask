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
    /// Settings for the <see cref="AzureEventHubsOrchestrationService"/> class.
    /// </summary>
    public class EventHubsOrchestrationServiceSettings
    {
        /// <summary>
        /// Gets or sets the name of the task hub.
        /// </summary>
        public string TaskHubName { get; set; }

        /// <summary>
        /// Gets or sets the Azure Event Hubs connection string.
        /// </summary>
        public string EventHubsConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the Azure Storage connection string.
        /// </summary>
        public string StorageConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the name of the Azure Storage lease container.
        /// </summary>
        public string LeaseContainerName { get; set; }

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
        public static EventHubsOrchestrationServiceSettings Validate(
            EventHubsOrchestrationServiceSettings settings)
        {
            if (settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }

            if (string.IsNullOrEmpty(settings.EventHubsConnectionString))
            {
                throw new ArgumentNullException(nameof(settings.EventHubsConnectionString));
            }

            if (string.IsNullOrEmpty(settings.StorageConnectionString))
            {
                throw new ArgumentNullException(nameof(settings.StorageConnectionString));
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
    }
}
