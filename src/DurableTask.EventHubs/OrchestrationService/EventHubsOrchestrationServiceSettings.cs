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
    using DurableTask.Core;
    using Microsoft.Azure.EventHubs;

    /// <summary>
    /// Settings for the <see cref="EventHubsOrchestrationService"/> class.
    /// </summary>
    public class EventHubsOrchestrationServiceSettings
    {
        /// <summary>
        /// Gets or sets the connection string for the event hubs namespace.
        /// Can be a real connection string, or of the form "Emulator:n" where n is the number of partitions
        /// </summary>
        public string EventHubsConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the connection string for the Azure storage used for leases and checkpoints.
        /// </summary>
        public string StorageConnectionString { get; set; }

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
        ///  Should we carry over unexecuted raised events to the next iteration of an orchestration on ContinueAsNew
        /// </summary>
        public BehaviorOnContinueAsNew EventBehaviourForContinueAsNew { get; set; } = BehaviorOnContinueAsNew.Carryover;

        /// <summary>
        ///  Whether to keep the orchestration service running even if stop is called.
        ///  This is useful in a testing scenario, due to the inordinate time spent when shutting down EventProcessorHost.
        /// </summary>
        public bool KeepServiceRunning { get; set; } = false;

        /// <inheritdoc/>
        public override bool Equals(object obj)
        {
            if (!(obj is EventHubsOrchestrationServiceSettings other))
                return false;

            return
                (this.EventHubsConnectionString,
                this.StorageConnectionString,
                this.MaxConcurrentTaskActivityWorkItems,
                this.MaxConcurrentTaskOrchestrationWorkItems,
                this.EventBehaviourForContinueAsNew,
                this.KeepServiceRunning)
                ==
                (other.EventHubsConnectionString,
                other.StorageConnectionString,
                other.MaxConcurrentTaskActivityWorkItems,
                other.MaxConcurrentTaskOrchestrationWorkItems,
                other.EventBehaviourForContinueAsNew,
                other.KeepServiceRunning);
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return (this.EventHubsConnectionString,
                this.StorageConnectionString,
                this.MaxConcurrentTaskActivityWorkItems,
                this.MaxConcurrentTaskOrchestrationWorkItems,
                this.EventBehaviourForContinueAsNew,
                this.KeepServiceRunning).GetHashCode();
        }

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

            if (string.IsNullOrEmpty(settings.EventHubsConnectionString))
            {
                throw new ArgumentNullException(nameof(settings.EventHubsConnectionString));
            }

            if (settings.UseEmulatedBackend)
            {
                var numberPartitions = settings.EmulatedPartitions;
                if (numberPartitions < 1 || numberPartitions > 32)
                {
                    throw new ArgumentOutOfRangeException(nameof(settings.EventHubsConnectionString));
                }
            }
            else
            {
                if (string.IsNullOrEmpty(settings.EventHubsNamespaceName))
                {
                    throw new FormatException(nameof(settings.EventHubsConnectionString));
                }

                if (string.IsNullOrEmpty(settings.StorageConnectionString))
                {
                    throw new ArgumentNullException(nameof(settings.StorageConnectionString));
                }
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
        /// Bypasses event hubs and uses in-memory emulation instead.
        /// </summary>
        public bool UseEmulatedBackend => (this.EventHubsConnectionString.StartsWith("Emulator:"));

        /// <summary>
        /// Gets the number of partitions when using the emulator
        /// </summary>
        public uint EmulatedPartitions => uint.Parse(this.EventHubsConnectionString.Substring(9));

        /// <summary>
        /// Returns the name of the eventhubs namespace
        /// </summary>
        public string EventHubsNamespaceName
        {
            get
            {
                var builder = new EventHubsConnectionStringBuilder(this.EventHubsConnectionString);
                var host = builder.Endpoint.Host;
                return host.Substring(0, host.IndexOf('.'));
            }
        }
    }
}
