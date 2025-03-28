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

namespace DurableTask.Core.Tracing
{
    internal class TraceActivityConstants
    {
        public const string Client = "client";
        public const string Orchestration = "orchestration";
        public const string Activity = "activity";
        public const string Event = "event";
        public const string Timer = "timer";
        public const string Entity = "entity";

        public const string CreateOrchestration = "create_orchestration";
        public const string OrchestrationEvent = "orchestration_event";

        public const string CallEntity = "call_entity";
        public const string SignalEntity = "signal_entity";
    }
}
