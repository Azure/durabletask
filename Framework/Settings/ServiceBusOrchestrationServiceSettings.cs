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

namespace DurableTask.Settings
{
    using DurableTask.Common;

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
            TaskOrchestrationDispatcherSettings = new TaskOrchestrationDispatcherSettings();
            TaskActivityDispatcherSettings = new TaskActivityDispatcherSettings();
            TrackingDispatcherSettings = new TrackingDispatcherSettings();
            MessageCompressionSettings = new CompressionSettings
            {
                Style = CompressionStyle.Never,
                ThresholdInBytes = 0
            };
        }

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
        /// </summary>
        public CompressionSettings MessageCompressionSettings { get; set; }

        internal ServiceBusOrchestrationServiceSettings Clone()
        {
            var clonedSettings = new ServiceBusOrchestrationServiceSettings();

            clonedSettings.TaskOrchestrationDispatcherSettings = TaskOrchestrationDispatcherSettings.Clone();
            clonedSettings.TaskActivityDispatcherSettings = TaskActivityDispatcherSettings.Clone();
            clonedSettings.TrackingDispatcherSettings = TrackingDispatcherSettings.Clone();

            clonedSettings.MessageCompressionSettings = MessageCompressionSettings;

            return clonedSettings;
        }
    }
}