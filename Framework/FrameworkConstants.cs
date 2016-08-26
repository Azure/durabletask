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

namespace DurableTask
{
    internal class FrameworkConstants
    {
        // name format constants
        public const string OrchestratorEndpointFormat = "{0}/orchestrator";
        public const string WorkerEndpointFormat = "{0}/worker";
        public const string TrackingEndpointFormat = "{0}/tracking";

        // tracking constants
        public const string TaskMessageContentType = "TaskMessage";
        public const string StateMessageContentType = "StateMessage";
        public const string HistoryEventIndexPropertyName = "HistoryEventIndex";

        public const int FakeTimerIdToSplitDecision = -100;
        public const int MaxDeliveryCount = 10;

        // task orchestration dispatcher default constants
        public const int OrchestrationTransientErrorBackOffSecs = 10;
        public const int OrchestrationNonTransientErrorBackOffSecs = 120;
        public const int OrchestrationDefaultMaxConcurrentItems = 100;

        // task activity dispatcher default constants
        public const int ActivityTransientErrorBackOffSecs = 10;
        public const int ActivityNonTransientErrorBackOffSecs = 120;
        public const int ActivityDefaultMaxConcurrentItems = 10;

        // tracking dispatcher default constants
        public const int TrackingTransientErrorBackOffSecs = 10;
        public const int TrackingNonTransientErrorBackOffSecs = 120;
        public const int TrackingDefaultMaxConcurrentItems = 20;

        // message content type constants
        public const string CompressionTypePropertyName = "CompressionType";
        public const string CompressionTypeGzipPropertyValue = "gzip";
        public const string CompressionTypeNonePropertyValue = "none";

        // instance store constants
        public const int MaxStringLengthForAzureTableColumn = 1024 * 15; // cut off at 15k * 2 bytes 
    }
}