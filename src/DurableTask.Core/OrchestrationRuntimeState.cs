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
    using DurableTask.Core.Tracing;

    /// <summary>
    /// Represents the runtime state of an orchestration
    /// </summary>
    public class OrchestrationRuntimeState
    {
        /// <summary>
        /// List of all history events for this runtime state
        /// </summary>
        public IList<HistoryEvent> Events { get; }

        /// <summary>
        /// List of new events added during an execution to keep track of the new events that were added during a particular execution 
        /// should not be serialized
        /// </summary>
        public IList<HistoryEvent> NewEvents { get; }

        readonly ISet<int> completedEventIds;

        /// <summary>
        /// Compressed size of the serialized state
        /// </summary>
        public long CompressedSize;

        ExecutionCompletedEvent ExecutionCompletedEvent { get; set; }

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
            completedEventIds = new HashSet<int>();

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
        public DateTime CompletedTime => ExecutionCompletedEvent?.Timestamp ?? Utils.DateTimeSafeMaxValue;

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
        /// Gets the serialized output of the ExecutionCompletedEvent if completed else null
        /// </summary>
        public string Output => ExecutionCompletedEvent?.Result;

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
        /// Gets the orchestration version of the ExecutionStartedEvent
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
        // This gets called by json.net for deserialization, we can't assert if there is no ExecutionStartedEvent
        public IDictionary<string, string> Tags => ExecutionStartedEvent?.Tags;

        /// <summary>
        /// Gets the status of the orchestration
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
        public OrchestrationInstance OrchestrationInstance => ExecutionStartedEvent?.OrchestrationInstance;

        /// <summary>
        /// Gets the ParentInstance of the ExecutionStartedEvent else null
        /// </summary>
        public ParentInstance ParentInstance => ExecutionStartedEvent?.ParentInstance;

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
            if (IsDuplicateEvent(historyEvent))
            {
                return;
            }

            Events.Add(historyEvent);

            if (isNewEvent)
            {
                NewEvents.Add(historyEvent);
            }

            SetMarkerEvents(historyEvent);
        }

        bool IsDuplicateEvent(HistoryEvent historyEvent)
        {
            if (historyEvent.EventId >= 0 &&
                historyEvent.EventType == EventType.TaskCompleted &&
                !completedEventIds.Add(historyEvent.EventId))
            {
                TraceHelper.Trace(TraceEventType.Warning, 
                    "OrchestrationRuntimeState-DuplicateEvent", 
                    "The orchestration {0} has already seen a completed task with id {1}.",
                    this.OrchestrationInstance.InstanceId,
                    historyEvent.EventId);
                return true;
            }
            return false;
        }

        void SetMarkerEvents(HistoryEvent historyEvent)
        {
            if (historyEvent is ExecutionStartedEvent startedEvent)
            {
                if (ExecutionStartedEvent != null)
                {
                    throw new InvalidOperationException(
                        "Multiple ExecutionStartedEvent found, potential corruption in state storage");
                }

                ExecutionStartedEvent = startedEvent;
            }
            else if (historyEvent is ExecutionCompletedEvent completedEvent)
            {
                if (ExecutionCompletedEvent != null)
                {
                    throw new InvalidOperationException(
                        "Multiple ExecutionCompletedEvent found, potential corruption in state storage");
                }

                ExecutionCompletedEvent = completedEvent;
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

            if (evt is TaskScheduledEvent taskScheduledEvent)
            {
                returnedEvent = new TaskScheduledEvent(taskScheduledEvent.EventId)
                {
                    Timestamp = taskScheduledEvent.Timestamp,
                    IsPlayed = taskScheduledEvent.IsPlayed,
                    Name = taskScheduledEvent.Name,
                    Version = taskScheduledEvent.Version,
                    Input = "[..snipped..]",
                };
            }
            else if (evt is TaskCompletedEvent taskCompletedEvent)
            {
                returnedEvent = new TaskCompletedEvent(taskCompletedEvent.EventId, taskCompletedEvent.TaskScheduledId, "[..snipped..]")
                {
                    Timestamp = taskCompletedEvent.Timestamp,
                    IsPlayed = taskCompletedEvent.IsPlayed,
                };
            }
            else if (evt is SubOrchestrationInstanceCreatedEvent subOrchestrationInstanceCreatedEvent)
            {
                returnedEvent = new SubOrchestrationInstanceCreatedEvent(subOrchestrationInstanceCreatedEvent.EventId)
                {
                    Timestamp = subOrchestrationInstanceCreatedEvent.Timestamp,
                    IsPlayed = subOrchestrationInstanceCreatedEvent.IsPlayed,
                    Name = subOrchestrationInstanceCreatedEvent.Name,
                    Version = subOrchestrationInstanceCreatedEvent.Version,
                    Input = "[..snipped..]",
                };
            }
            else if (evt is SubOrchestrationInstanceCompletedEvent subOrchestrationInstanceCompletedEvent)
            {
                returnedEvent = new SubOrchestrationInstanceCompletedEvent(subOrchestrationInstanceCompletedEvent.EventId,
                    subOrchestrationInstanceCompletedEvent.TaskScheduledId, "[..snipped..]")
                {
                    Timestamp = subOrchestrationInstanceCompletedEvent.Timestamp,
                    IsPlayed = subOrchestrationInstanceCompletedEvent.IsPlayed,
                };
            }
            else if (evt is TaskFailedEvent taskFailedEvent)
            {
                returnedEvent = new TaskFailedEvent(taskFailedEvent.EventId,
                    taskFailedEvent.TaskScheduledId, taskFailedEvent.Reason, "[..snipped..]")
                {
                    Timestamp = taskFailedEvent.Timestamp,
                    IsPlayed = taskFailedEvent.IsPlayed,
                };
            }
            else if (evt is SubOrchestrationInstanceFailedEvent subOrchestrationInstanceFailedEvent)
            {
                returnedEvent = new SubOrchestrationInstanceFailedEvent(subOrchestrationInstanceFailedEvent.EventId,
                    subOrchestrationInstanceFailedEvent.TaskScheduledId, subOrchestrationInstanceFailedEvent.Reason, "[..snipped..]")
                {
                    Timestamp = subOrchestrationInstanceFailedEvent.Timestamp,
                    IsPlayed = subOrchestrationInstanceFailedEvent.IsPlayed,
                };
            }
            else if (evt is ExecutionStartedEvent executionStartedEvent)
            {
                returnedEvent = new ExecutionStartedEvent(executionStartedEvent.EventId, "[..snipped..]")
                {
                    Timestamp = executionStartedEvent.Timestamp,
                    IsPlayed = executionStartedEvent.IsPlayed,
                };
            }
            else if (evt is ExecutionCompletedEvent executionCompletedEvent)
            {
                returnedEvent = new ExecutionCompletedEvent(executionCompletedEvent.EventId, "[..snipped..]",
                    executionCompletedEvent.OrchestrationStatus)
                {
                    Timestamp = executionCompletedEvent.Timestamp,
                    IsPlayed = executionCompletedEvent.IsPlayed,
                };
            }
            else if (evt is ExecutionTerminatedEvent executionTerminatedEvent)
            {
                returnedEvent = new ExecutionTerminatedEvent(executionTerminatedEvent.EventId, "[..snipped..]")
                {
                    Timestamp = executionTerminatedEvent.Timestamp,
                    IsPlayed = executionTerminatedEvent.IsPlayed,
                };
            }
            // ContinueAsNewEvent is covered by the ExecutionCompletedEvent block

            return returnedEvent;
        }
    }
}