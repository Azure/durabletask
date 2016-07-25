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

namespace DurableTask.History
{
    using System;
    using Newtonsoft.Json.Linq;
    using DurableTask.Serializing;

    // AFFANDAR : TODO : add the remaining event
    internal class HistoryEventConverter : JsonCreationConverter<HistoryEvent>
    {
        protected override HistoryEvent CreateObject(Type objectType, JObject jobject)
        {
            JToken eventType;
            if (jobject.TryGetValue("EventType", StringComparison.OrdinalIgnoreCase, out eventType))
            {
                var type = (EventType)int.Parse((string)eventType);

                //ExecutionStarted,
                //ExecutionCompleted,
                //ExecutionTerminated,
                //TaskScheduled,
                //TaskCompleted,
                //TaskFailed,
                //SubOrchestrationInstanceCreated,
                //SubOrchestrationInstanceCompleted,
                //SubOrchestrationInstanceFailed,
                //TimerCreated,
                //TimerFired,
                //OrchestratorStarted,
                //OrchestratorCompleted,
                //EventRaised,
                //ContinueAsNew,
                //GenericEvent,
                //HistoryState,

                switch (type)
                {
                    case EventType.ExecutionStarted:
                        return new ExecutionStartedEvent();
                    case EventType.ExecutionCompleted:
                        return new ExecutionCompletedEvent();
                    case EventType.ExecutionTerminated:
                        return new ExecutionTerminatedEvent();
                    case EventType.TaskScheduled:
                        return new TaskScheduledEvent();
                    case EventType.TaskCompleted:
                        return new TaskCompletedEvent();
                    case EventType.TaskFailed:
                        return new TaskFailedEvent();
                    case EventType.SubOrchestrationInstanceCreated:
                        return new SubOrchestrationInstanceCreatedEvent();
                    case EventType.SubOrchestrationInstanceCompleted:
                        return new SubOrchestrationInstanceCompletedEvent();
                    case EventType.SubOrchestrationInstanceFailed:
                        return new SubOrchestrationInstanceFailedEvent();
                    case EventType.TimerCreated:
                        return new TimerCreatedEvent();
                    case EventType.TimerFired:
                        return new TimerFiredEvent();
                    case EventType.OrchestratorStarted:
                        return new OrchestratorStartedEvent();
                    case EventType.OrchestratorCompleted:
                        return new OrchestratorCompletedEvent();
                    case EventType.EventRaised:
                        return new EventRaisedEvent();
                    case EventType.ContinueAsNew:
                        return new ContinueAsNewEvent();
                    case EventType.GenericEvent:
                        return new GenericEvent();
                    case EventType.HistoryState:
                        return new HistoryStateEvent();
                    default:
                        throw new NotSupportedException("Unrecognized action type.");
                }
            }
            throw new NotSupportedException("eventType not provided.");
        }
    }
}