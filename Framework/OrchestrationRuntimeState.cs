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
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Common;
    using History;

    public class OrchestrationRuntimeState
    {
        public readonly IList<HistoryEvent> Events;

        // to keep track of the new events that were added during a particular execution
        // should not be serialized
        public readonly IList<HistoryEvent> NewEvents;
        public long CompressedSize;

        ExecutionCompletedEvent ExecutionCompletedEvent;
        ExecutionStartedEvent ExecutionStartedEvent;

        public long Size;
        public string Status;

        public OrchestrationRuntimeState()
            : this(null)
        {
        }

        public OrchestrationRuntimeState(IList<HistoryEvent> events)
        {
            Events = new List<HistoryEvent>();
            NewEvents = new List<HistoryEvent>();
            if (events != null && events.Count > 0)
            {
                foreach (HistoryEvent ev in events)
                {
                    AddEvent(ev, false);
                }
            }
        }

        public DateTime CreatedTime
        {
            get
            {
                Debug.Assert(ExecutionStartedEvent != null);
                return ExecutionStartedEvent.Timestamp;
            }
        }

        public DateTime CompletedTime
        {
            get
            {
                return ExecutionCompletedEvent == null
                    ? Utils.DateTimeSafeMaxValue
                    : ExecutionCompletedEvent.Timestamp;
            }
        }

        public string Input
        {
            get
            {
                Debug.Assert(ExecutionStartedEvent != null);
                return ExecutionStartedEvent.Input;
            }
        }

        public String Output
        {
            get { return ExecutionCompletedEvent == null ? null : ExecutionCompletedEvent.Result; }
        }

        public string Name
        {
            get
            {
                Debug.Assert(ExecutionStartedEvent != null);
                return ExecutionStartedEvent.Name;
            }
        }

        public string Version
        {
            get
            {
                Debug.Assert(ExecutionStartedEvent != null);
                return ExecutionStartedEvent.Version;
            }
        }

        public string Tags
        {
            get
            {
                Debug.Assert(ExecutionStartedEvent != null);
                return ExecutionStartedEvent.Tags;
            }
        }

        public OrchestrationStatus OrchestrationStatus
        {
            get
            {
                Debug.Assert(ExecutionStartedEvent != null);

                if (ExecutionCompletedEvent != null)
                {
                    return ExecutionCompletedEvent.OrchestrationStatus;
                }

                return OrchestrationStatus.Running;
            }
        }

        public OrchestrationInstance OrchestrationInstance
        {
            get { return ExecutionStartedEvent == null ? null : ExecutionStartedEvent.OrchestrationInstance; }
        }

        public ParentInstance ParentInstance
        {
            get { return ExecutionStartedEvent == null ? null : ExecutionStartedEvent.ParentInstance; }
        }

        public void AddEvent(HistoryEvent historyEvent)
        {
            AddEvent(historyEvent, true);
        }

        void AddEvent(HistoryEvent historyEvent, bool isNewEvent)
        {
            Events.Add(historyEvent);
            if (isNewEvent)
            {
                NewEvents.Add(historyEvent);
            }
            SetMarkerEvents(historyEvent);
        }

        void SetMarkerEvents(HistoryEvent historyEvent)
        {
            if (historyEvent is ExecutionStartedEvent)
            {
                if (ExecutionStartedEvent != null)
                {
                    throw new InvalidOperationException(
                        "Multiple ExecutionStartedEvent found, potential corruption in state storage");
                }
                ExecutionStartedEvent = (ExecutionStartedEvent) historyEvent;
            }
            else if (historyEvent is ExecutionCompletedEvent)
            {
                if (ExecutionCompletedEvent != null)
                {
                    throw new InvalidOperationException(
                        "Multiple ExecutionCompletedEvent found, potential corruption in state storage");
                }
                ExecutionCompletedEvent = (ExecutionCompletedEvent) historyEvent;
            }
        }

        public OrchestrationRuntimeStateDump GetOrchestrationRuntimeStateDump()
        {
            var runtimeStateDump = new OrchestrationRuntimeStateDump
            {
                Events = new List<HistoryEvent>(),
                NewEvents = new List<HistoryEvent>(),
            };

            foreach (HistoryEvent evt in Events)
            {
                HistoryEvent abridgeEvent = GenerateAbridgedEvent(evt);
                runtimeStateDump.Events.Add(abridgeEvent);
            }

            foreach (HistoryEvent evt in NewEvents)
            {
                HistoryEvent abridgeEvent = GenerateAbridgedEvent(evt);
                runtimeStateDump.NewEvents.Add(abridgeEvent);
            }

            return runtimeStateDump;
        }

        HistoryEvent GenerateAbridgedEvent(HistoryEvent evt)
        {
            HistoryEvent returnedEvent = evt;

            if (evt is TaskScheduledEvent)
            {
                var sourceEvent = (TaskScheduledEvent) evt;
                returnedEvent = new TaskScheduledEvent(sourceEvent.EventId)
                {
                    Timestamp = sourceEvent.Timestamp,
                    IsPlayed = sourceEvent.IsPlayed,
                    Name = sourceEvent.Name,
                    Version = sourceEvent.Version,
                    Input = "[..snipped..]",
                };
            }
            else if (evt is TaskCompletedEvent)
            {
                var sourceEvent = (TaskCompletedEvent) evt;
                returnedEvent = new TaskCompletedEvent(sourceEvent.EventId, sourceEvent.TaskScheduledId, "[..snipped..]")
                {
                    Timestamp = sourceEvent.Timestamp,
                    IsPlayed = sourceEvent.IsPlayed,
                };
            }
            else if (evt is SubOrchestrationInstanceCreatedEvent)
            {
                var sourceEvent = (SubOrchestrationInstanceCreatedEvent) evt;
                returnedEvent = new SubOrchestrationInstanceCreatedEvent(sourceEvent.EventId)
                {
                    Timestamp = sourceEvent.Timestamp,
                    IsPlayed = sourceEvent.IsPlayed,
                    Name = sourceEvent.Name,
                    Version = sourceEvent.Version,
                    Input = "[..snipped..]",
                };
            }
            else if (evt is SubOrchestrationInstanceCompletedEvent)
            {
                var sourceEvent = (SubOrchestrationInstanceCompletedEvent) evt;
                returnedEvent = new SubOrchestrationInstanceCompletedEvent(sourceEvent.EventId,
                    sourceEvent.TaskScheduledId, "[..snipped..]")
                {
                    Timestamp = sourceEvent.Timestamp,
                    IsPlayed = sourceEvent.IsPlayed,
                };
            }
            else if (evt is TaskFailedEvent)
            {
                var sourceEvent = (TaskFailedEvent) evt;
                returnedEvent = new TaskFailedEvent(sourceEvent.EventId,
                    sourceEvent.TaskScheduledId, sourceEvent.Reason, "[..snipped..]")
                {
                    Timestamp = sourceEvent.Timestamp,
                    IsPlayed = sourceEvent.IsPlayed,
                };
            }
            else if (evt is SubOrchestrationInstanceFailedEvent)
            {
                var sourceEvent = (SubOrchestrationInstanceFailedEvent) evt;
                returnedEvent = new SubOrchestrationInstanceFailedEvent(sourceEvent.EventId,
                    sourceEvent.TaskScheduledId, sourceEvent.Reason, "[..snipped..]")
                {
                    Timestamp = sourceEvent.Timestamp,
                    IsPlayed = sourceEvent.IsPlayed,
                };
            }
            else if (evt is ExecutionStartedEvent)
            {
                var sourceEvent = (ExecutionStartedEvent) evt;
                returnedEvent = new ExecutionStartedEvent(sourceEvent.EventId, "[..snipped..]")
                {
                    Timestamp = sourceEvent.Timestamp,
                    IsPlayed = sourceEvent.IsPlayed,
                };
            }
            else if (evt is ExecutionCompletedEvent)
            {
                var sourceEvent = (ExecutionCompletedEvent) evt;
                returnedEvent = new ExecutionCompletedEvent(sourceEvent.EventId, "[..snipped..]",
                    sourceEvent.OrchestrationStatus)
                {
                    Timestamp = sourceEvent.Timestamp,
                    IsPlayed = sourceEvent.IsPlayed,
                };
            }
            else if (evt is ExecutionTerminatedEvent)
            {
                var sourceEvent = (ExecutionTerminatedEvent) evt;
                returnedEvent = new ExecutionTerminatedEvent(sourceEvent.EventId, "[..snipped..]")
                {
                    Timestamp = sourceEvent.Timestamp,
                    IsPlayed = sourceEvent.IsPlayed,
                };
            }
            // ContinueAsNewEvent is covered by the ExecutionCompletedEvent block

            return returnedEvent;
        }

        public class OrchestrationRuntimeStateDump
        {
            public IList<HistoryEvent> Events;
            public IList<HistoryEvent> NewEvents;
        }
    }
}