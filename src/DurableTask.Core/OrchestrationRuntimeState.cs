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
    using System.Collections.Generic;
    using System.Diagnostics;
    using DurableTask.Core.Common;
    using DurableTask.Core.History;

    /// <summary>
    /// Represents the runtime state of an orchestration
    /// </summary>
    public class OrchestrationRuntimeState
    {
        /// <summary>
        /// List of all history events for this runtime state
        /// </summary>
        public readonly IList<HistoryEvent> Events;

        /// <summary>
        /// List of new events added during an execution to keep track of the new events that were added during a particular execution 
        /// should not be serialized
        /// </summary>
        public readonly IList<HistoryEvent> NewEvents;
        
        /// <summary>
        /// Compressed size of the serialized state
        /// </summary>
        public long CompressedSize;

        ExecutionCompletedEvent ExecutionCompletedEvent;

        /// <summary>
        /// Size of the serialized state (uncompressed)
        /// </summary>
        public long Size;

        /// <summary>
        /// The string status of the runtime state
        /// </summary>
        public string Status;

        /// <summary>
        /// Creates a new instance of the OrchestrationRuntimeState
        /// </summary>
        public OrchestrationRuntimeState()
            : this(null)
        {
        }

        /// <summary>
        /// Creates a new instance of the OrchestrationRuntimeState with the supplied events
        /// </summary>
        /// <param name="events">List of events for this runtime state</param>
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

        /// <summary>
        /// Gets the execution started event
        /// </summary>
        public ExecutionStartedEvent ExecutionStartedEvent
        {
            get; private set;
        }

        /// <summary>
        /// Gets the created time of the ExecutionStartedEvent
        /// </summary>
        public DateTime CreatedTime
        {
            get
            {
                Debug.Assert(ExecutionStartedEvent != null);
                return ExecutionStartedEvent.Timestamp;
            }
        }

        /// <summary>
        /// Gets the created time of the ExecutionCompletedEvent if completed else a safe (from timezone shift) max datetime
        /// </summary>
        public DateTime CompletedTime
        {
            get
            {
                return ExecutionCompletedEvent == null
                    ? Utils.DateTimeSafeMaxValue
                    : ExecutionCompletedEvent.Timestamp;
            }
        }

        /// <summary>
        /// Gets the serialized input of the ExecutionStartedEvent
        /// </summary>
        public string Input
        {
            get
            {
                Debug.Assert(ExecutionStartedEvent != null);
                return ExecutionStartedEvent.Input;
            }
        }

        /// <summary>
        /// Gets the serialized output of the ExecutionCompletedEvent if compeleted else null
        /// </summary>
        public String Output
        {
            get { return ExecutionCompletedEvent == null ? null : ExecutionCompletedEvent.Result; }
        }

        /// <summary>
        /// Gets the orchestration name of the ExecutionStartedEvent
        /// </summary>
        public string Name
        {
            get
            {
                Debug.Assert(ExecutionStartedEvent != null);
                return ExecutionStartedEvent.Name;
            }
        }

        /// <summary>
        /// Gets the orchestraion version of the ExecutionStartedEvent
        /// </summary>
        public string Version
        {
            get
            {
                Debug.Assert(ExecutionStartedEvent != null);
                return ExecutionStartedEvent.Version;
            }
        }

        /// <summary>
        /// Gets the tags from the ExecutionStartedEvent
        /// </summary>
        public IDictionary<string, string> Tags
        {
            get
            {
                // This gets called by json.net for deserialization, we can't assert if there is no ExecutionStartedEvent
                return ExecutionStartedEvent?.Tags;
            }
        }

        /// <summary>
        /// Gets the status of the orchestation
        /// If complete then the status from the ExecutionCompletedEvent else Running.
        /// </summary>
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

        /// <summary>
        /// Gets the OrchestrationInstance of the ExecutionStartedEvent else null
        /// </summary>
        public OrchestrationInstance OrchestrationInstance
        {
            get { return ExecutionStartedEvent == null ? null : ExecutionStartedEvent.OrchestrationInstance; }
        }

        /// <summary>
        /// Gets the ParentInstance of the ExecutionStartedEvent else null
        /// </summary>
        public ParentInstance ParentInstance
        {
            get { return ExecutionStartedEvent == null ? null : ExecutionStartedEvent.ParentInstance; }
        }

        /// <summary>
        /// Adds a new history event to the Events list and NewEvents list
        /// </summary>
        /// <param name="historyEvent">The new history event to add</param>
        public void AddEvent(HistoryEvent historyEvent)
        {
            AddEvent(historyEvent, true);
        }

        /// <summary>
        /// Adds a new history event to the Events list and optionally NewEvents list
        /// </summary>
        /// <param name="historyEvent">The history event to add</param>
        /// <param name="isNewEvent">Flag indicating whether this is a new event or not</param>
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

        /// <summary>
        /// Gets a statedump of the current list of events
        /// </summary>
        /// <returns></returns>
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
    }
}