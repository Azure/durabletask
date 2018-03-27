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

namespace DurableTask.Core
{
    using System;

    /// <summary>
    /// Shared framework constants
    /// </summary>
    public class FrameworkConstants
    {
        // tracking constants
        /// <summary>
        /// The content type of a Task Message
        /// </summary>
        public const string TaskMessageContentType = "TaskMessage";

        /// <summary>
        /// The content type of a State Message
        /// </summary>
        public const string StateMessageContentType = "StateMessage";

        /// <summary>
        /// The property name of a history event index
        /// </summary>
        public const string HistoryEventIndexPropertyName = "HistoryEventIndex";

        /// <summary>
        /// Id for a fake timer event
        /// </summary>
        public const int FakeTimerIdToSplitDecision = -100;

        // task orchestration dispatcher default constants
        /// <summary>
        /// The default error backoff for transient errors task orchestrations in seconds
        /// </summary>
        public const int OrchestrationTransientErrorBackOffSecs = 10;

        /// <summary>
        /// The default error backoff for non-transient errors task orchestrations in seconds
        /// </summary>
        public const int OrchestrationNonTransientErrorBackOffSecs = 120;

        /// <summary>
        /// The default number of orchestration dispatchers
        /// </summary>
        public const int OrchestrationDefaultDispatcherCount = 1;

        /// <summary>
        /// The default max concurrent orchestration work items
        /// </summary>
        public const int OrchestrationDefaultMaxConcurrentItems = 100;

        // task activity dispatcher default constants
        /// <summary>
        /// The default error backoff for transient errors task activities in seconds
        /// </summary>
        public const int ActivityTransientErrorBackOffSecs = 10;

        /// <summary>
        /// The default error backoff for non-transient errors task activities in seconds
        /// </summary>
        public const int ActivityNonTransientErrorBackOffSecs = 120;

        /// <summary>
        /// The default number of activity dispatchers
        /// </summary>
        public const int ActivityDefaultDispatcherCount = 1;

        /// <summary>
        /// The default max concurrent activity work items
        /// </summary>
        public const int ActivityDefaultMaxConcurrentItems = 10;

        // tracking dispatcher default constants
        /// <summary>
        /// The default error backoff for transient errors tracking activities in seconds
        /// </summary>
        public const int TrackingTransientErrorBackOffSecs = 10;

        /// <summary>
        /// The default error backoff for non-transient errors tracking activities in seconds
        /// </summary>
        public const int TrackingNonTransientErrorBackOffSecs = 120;

        /// <summary>
        /// The default number of tracking dispatchers
        /// </summary>
        public const int TrackingDefaultDispatcherCount = 1;

        /// <summary>
        /// The default max concurrent tracking work items
        /// </summary>
        public const int TrackingDefaultMaxConcurrentItems = 20;

        /// <summary>
        /// The default setting for enabling tracking history events
        /// </summary>
        public const bool TrackingTrackHistoryEvents = true;

        // Jumpstart constants
        /// <summary>
        /// The default timespan for the JumpStart interval
        /// </summary>
        public static TimeSpan JumpStartDefaultInterval = TimeSpan.FromSeconds(5);

        /// <summary>
        /// The default timespan for the JumpStart ignore window
        /// </summary>
        public static TimeSpan JumpStartDefaultIgnoreWindow = TimeSpan.FromMinutes(10);

        // message content type constants
        /// <summary>
        /// The property name for compression type
        /// </summary>
        public const string CompressionTypePropertyName = "CompressionType";

        /// <summary>
        /// The property value for compression type gzip
        /// </summary>
        public const string CompressionTypeGzipPropertyValue = "gzip";

        /// <summary>
        /// The property value for compression type none
        /// </summary>
        public const string CompressionTypeNonePropertyValue = "none";

        // default settings for message size
        /// <summary>
        /// The default max message size before overflow
        /// </summary>
        public const int MessageOverflowThresholdInBytesDefault = 170 * 1024;

        /// <summary>
        /// The default max allowed message size
        /// </summary>
        public const int MessageMaxSizeInBytesDefault = 10 * 1024 * 1024;

        // default settings for session size
        /// <summary>
        /// The default max session size before overflow
        /// </summary>
        public const int SessionOverflowThresholdInBytesDefault = 230 * 1024;

        /// <summary>
        /// The default max allowed session size
        /// </summary>
        public const int SessionMaxSizeInBytesDefault = 10 * 1024 * 1024;
    }
}