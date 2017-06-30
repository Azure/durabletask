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

namespace DurableTask.ServiceBus.Settings
{
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.Settings;

    /// <summary>
    ///     Configuration for various TaskHubWorker options
    /// </summary>
    public sealed class ServiceBusOrchestrationServiceSettings
    {
        /// <summary>
        ///     Create a TaskHubWorkerSettings object with default settings
        /// </summary>
        public ServiceBusOrchestrationServiceSettings()
        {
            MaxTaskOrchestrationDeliveryCount = ServiceBusConstants.MaxDeliveryCount;
            MaxTaskActivityDeliveryCount = ServiceBusConstants.MaxDeliveryCount;
            MaxTrackingDeliveryCount = ServiceBusConstants.MaxDeliveryCount;
            TaskOrchestrationDispatcherSettings = new TaskOrchestrationDispatcherSettings();
            TaskActivityDispatcherSettings = new TaskActivityDispatcherSettings();
            TrackingDispatcherSettings = new TrackingDispatcherSettings();
            JumpStartSettings = new JumpStartSettings();
            SessionSettings = new ServiceBusSessionSettings();
            MessageSettings = new ServiceBusMessageSettings();

            MessageCompressionSettings = new CompressionSettings
            {
                Style = CompressionStyle.Never,
                ThresholdInBytes = 0
            };
        }

        /// <summary>
        ///     Maximum number of times the task orchestration dispatcher will try to
        ///     process an orchestration message before giving up
        /// </summary>
        public int MaxTaskOrchestrationDeliveryCount { get; set; }

        /// <summary>
        ///     Maximum number of times the task activity dispatcher will try to
        ///     process an orchestration message before giving up
        /// </summary>
        public int MaxTaskActivityDeliveryCount { get; set; }

        /// <summary>
        ///     Maximum number of times the tracking dispatcher will try to
        ///     process an orchestration message before giving up
        /// </summary>
        public int MaxTrackingDeliveryCount { get; set; }

        /// <summary>
        ///     Maximum queue size, in megabytes, for the service bus queues
        /// </summary>
        public long MaxQueueSizeInMegabytes { get; set; } = 1024L;

        /// <summary>
        /// Gets the message prefetch count
        /// </summary>
        public int PrefetchCount { get; } = 50;

        /// <summary>
        /// Gets the default interval in settings between retries
        /// </summary>
        public int IntervalBetweenRetriesSecs { get; } = 5;

        /// <summary>
        /// Gets the max retries
        /// </summary>
        public int MaxRetries { get; } = 5;

        /// <summary>
        /// Settings for the JumpStartManager
        /// </summary>
        public JumpStartSettings JumpStartSettings { get; private set; }

        /// <summary>
        ///     Settings to configure the Task Orchestration Dispatcher
        /// </summary>
        public TaskOrchestrationDispatcherSettings TaskOrchestrationDispatcherSettings { get; private set; }

        /// <summary>
        ///     Settings to configure the Task Activity Dispatcher
        /// </summary>
        public TaskActivityDispatcherSettings TaskActivityDispatcherSettings { get; private set; }

        /// <summary>
        ///     Settings to configure the Tracking Dispatcher
        /// </summary>
        public TrackingDispatcherSettings TrackingDispatcherSettings { get; private set; }

        /// <summary>
        ///     Enable compression of messages. Allows exchange of larger parameters and return values with activities at the cost
        ///     of additional CPU.
        ///     Default is false.
        ///     TODO: move this setting into ServiceBusSessionSettings and ServiceBusMessageSettings.
        /// </summary>
        public CompressionSettings MessageCompressionSettings { get; set; }

        /// <summary>
        ///     Settings to configure the session
        /// </summary>
        public ServiceBusSessionSettings SessionSettings { get; set; }

        /// <summary>
        ///     Settings to configure the message
        /// </summary>
        public ServiceBusMessageSettings MessageSettings { get; set; }
    }
}