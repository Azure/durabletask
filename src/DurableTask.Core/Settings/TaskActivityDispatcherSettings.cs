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

namespace DurableTask.Core.Settings
{
    /// <summary>
    ///     Settings to configure the Task Activity Dispatcher
    /// </summary>
    public class TaskActivityDispatcherSettings
    {
        /// <summary>
        ///     Creates a new instance of the TaskActivityDispatcherSettings with the default settings
        /// </summary>
        public TaskActivityDispatcherSettings()
        {
            TransientErrorBackOffSecs = FrameworkConstants.ActivityTransientErrorBackOffSecs;
            NonTransientErrorBackOffSecs = FrameworkConstants.ActivityNonTransientErrorBackOffSecs;
            DispatcherCount = FrameworkConstants.ActivityDefaultDispatcherCount;
            MaxConcurrentActivities = FrameworkConstants.ActivityDefaultMaxConcurrentItems;
        }

        /// <summary>
        ///     Time in seconds to wait before retrying on a transient error (e.g. communication exception). Default is 10s.
        /// </summary>
        public int TransientErrorBackOffSecs { get; set; }

        /// <summary>
        ///     Time in seconds to wait before retrying on a non-transient error. Default is 120s.
        /// </summary>
        public int NonTransientErrorBackOffSecs { get; set; }

        /// <summary>
        ///     How many dispatchers to create. Default is 1.
        /// </summary>
        public int DispatcherCount { get; set; }

        /// <summary>
        ///     How many activities to process concurrently. Default is 10.
        /// </summary>
        public int MaxConcurrentActivities { get; set; }

        internal TaskActivityDispatcherSettings Clone()
        {
            return new TaskActivityDispatcherSettings
            {
                TransientErrorBackOffSecs = TransientErrorBackOffSecs,
                NonTransientErrorBackOffSecs = NonTransientErrorBackOffSecs,
                MaxConcurrentActivities = MaxConcurrentActivities,
            };
        }
    }
}