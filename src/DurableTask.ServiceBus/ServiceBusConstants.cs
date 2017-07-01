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

namespace DurableTask.ServiceBus
{
    using System;

    internal class ServiceBusConstants
    {
        // name format constants
        public const string OrchestratorEndpointFormat = "{0}/orchestrator";
        public const string WorkerEndpointFormat = "{0}/worker";
        public const string TrackingEndpointFormat = "{0}/tracking";

        // tracking constants
        public const string TaskMessageContentType = "TaskMessage";
        public const string StateMessageContentType = "StateMessage";
        public const string HistoryEventIndexPropertyName = "HistoryEventIndex";

        public const int MaxDeliveryCount = 10;

        // message blob key in message property
        // this property is a key to the message blob when it exceeds the message limit
        public const string MessageBlobKey = "MessageBlobKey";

        // instance store constants
        public const int MaxStringLengthForAzureTableColumn = 1024 * 15; // cut off at 15k * 2 bytes
    }
}