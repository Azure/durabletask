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

using System;

namespace DurableTask
{
    /// <summary>
    ///     Configuration for various TaskHub settings
    /// </summary>
    //[Obsolete]
    public sealed class TaskHubDescription
    {
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
        ///     Creates a TaskHubDescription object with standard settings
        /// </summary>
        /// <returns></returns>
        public static TaskHubDescription CreateDefaultDescription()
        {
            return new TaskHubDescription
            {
                MaxTaskActivityDeliveryCount = FrameworkConstants.MaxDeliveryCount,
                MaxTaskOrchestrationDeliveryCount = FrameworkConstants.MaxDeliveryCount,
                MaxTrackingDeliveryCount = FrameworkConstants.MaxDeliveryCount,
            };
        }
    }
}