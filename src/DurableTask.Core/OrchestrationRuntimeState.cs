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
#nullable enable
namespace DurableTask.Core
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using DurableTask.Core.Common;
    using DurableTask.Core.History;
    using DurableTask.Core.Logging;
    using DurableTask.Core.Tracing;

    /// <summary>
    /// Represents the runtime state of an orchestration
    /// </summary>
    public class OrchestrationRuntimeState
    {
        private OrchestrationStatus orchestrationStatus;

        /// <summary>
        /// List of all history events for this runtime state.
        /// Note that this list is frequently a combination of <see cref="PastEvents"/> and <see cref="NewEvents"/>, but not always.
        /// </summary>
        public IList<HistoryEvent> Events { get; }

        /// <summary>
        /// List of new events added during an execution to keep track of the new events that were added during a particular execution.
        /// Should not be serialized.
        /// This list is NOT guaranteed to be a subset of <see cref="Events"/>.
        /// </summary>
        public IList<HistoryEvent> NewEvents { get; }

        /// <summary>
        /// A subset of <see cref="Events"/> that contains only events that have been previously played and should not be serialized.
        /// </summary>
        public IList<HistoryEvent> PastEvents { get; }

        readonly ISet<int> completedEventIds;

        /// <summary>
        /// Compressed size of the serialized state
        /// </summary>
        public long CompressedSize;

        /// <summary>
        /// Gets the execution completed event
        /// </summary>
        public ExecutionCompletedEvent? ExecutionCompletedEvent { get; set; }

        /// <summary>
        /// Size of the serialized state (uncompressed)
        /// </summary>
        public long Size;

        /// <summary>
        /// The string status of the runtime state
        /// </summary>
        public string? Status;

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
        public OrchestrationRuntimeState(IList<HistoryEvent>? events)
        {
            Events = events != null ? new List<HistoryEvent>(events.Count) : new List<HistoryEvent>();
            PastEvents = events != null ? new List<HistoryEvent>(events.Count) : new List<HistoryEvent>();
            NewEvents = new List<HistoryEvent>();
            completedEventIds = new HashSet<int>();
            orchestrationStatus = OrchestrationStatus.Running;

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
        public ExecutionStartedEvent? ExecutionStartedEvent
        {
            get; private set;
        }

        /// <summary>
        /// Gets the created time of the ExecutionStartedEvent
        /// </summary>
        public DateTime CreatedTime => GetExecutionStartedEventOrThrow().Timestamp;

        /// <summary>
        /// Gets the created time of the ExecutionCompletedEvent if completed else a safe (from timezone shift) max datetime
        /// </summary>
        public DateTime CompletedTime => ExecutionCompletedEvent?.Timestamp ?? Utils.DateTimeSafeMaxValue;

        /// <summary>
        /// Gets the serialized input of the ExecutionStartedEvent
        /// </summary>
        public string? Input => GetExecutionStartedEventOrThrow().Input;

        /// <summary>
        /// Gets the serialized output of the ExecutionCompletedEvent if completed else null
        /// </summary>
        public string? Output => ExecutionCompletedEvent?.FailureDetails?.ToString() ?? ExecutionCompletedEvent?.Result;

        /// <summary>
        /// Gets the structured failure details for this orchestration.
        /// </summary>
        /// <remarks>
        /// This property is set only when the orchestration fails and <see cref="TaskHubWorker.ErrorPropagationMode"/>
        /// is set to <see cref="ErrorPropagationMode.UseFailureDetails"/>.
        /// </remarks>
        public FailureDetails? FailureDetails => ExecutionCompletedEvent?.FailureDetails;

        /// <summary>
        /// Failure that caused an orchestrator to complete, if any.
        /// </summary>
        /// <remarks>
        /// This property was introduced to sanitize exceptions during logging. See it's usage in <see cref="LogEvents.OrchestrationCompleted"/>.
        /// </remarks>
        internal Exception? Exception { get; set; }

        /// <summary>
        /// Gets the orchestration name from the ExecutionStartedEvent
        /// </summary>
        public string Name => GetExecutionStartedEventOrThrow().Name;

        /// <summary>
        /// Gets the orchestration version of the ExecutionStartedEvent
        /// </summary>
        public string Version => GetExecutionStartedEventOrThrow().Version;

        /// <summary>
        /// Gets the tags from the ExecutionStartedEvent
        /// </summary>
        // This gets called by json.net for deserialization, we can't throw if there is no ExecutionStartedEvent
        public IDictionary<string, string>? Tags => ExecutionStartedEvent?.Tags;

        /// <summary>
        /// Gets the status of the orchestration
        /// If complete then the status from the ExecutionCompletedEvent else Running.
        /// </summary>
        public OrchestrationStatus OrchestrationStatus
        {
            get
            {
                GetExecutionStartedEventOrThrow();

                return orchestrationStatus;
            }
        }

        /// <summary>
        /// Gets the OrchestrationInstance of the ExecutionStartedEvent else null
        /// </summary>
        public OrchestrationInstance? OrchestrationInstance => ExecutionStartedEvent?.OrchestrationInstance;

        /// <summary>
        /// Gets the ParentInstance of the ExecutionStartedEvent else null
        /// </summary>
        public ParentInstance? ParentInstance => ExecutionStartedEvent?.ParentInstance;

        /// <summary>
        /// Gets a value indicating whether the orchestration state is valid.
        /// </summary>
        /// <remarks>
        /// An invalid orchestration runtime state means that the history is somehow corrupted.
        /// </remarks>
        public bool IsValid => 
            this.Events.Count == 0 ||
            this.Events.Count == 1 && this.Events[0].EventType == EventType.OrchestratorStarted ||
            this.ExecutionStartedEvent != null;

        /// <summary>
        /// Gets or sets a LogHelper instance that can be used to log messages.
        /// </summary>
        /// <remarks>
        /// Ideally, this would be set in the constructor but that would require a larger refactoring.
        /// </remarks>
        internal LogHelper? LogHelper { get; set; } = null;

        /// <summary>
        /// Adds a new history event to the Events list and NewEvents list
        /// </summary>
        /// <param name="historyEvent">The new history event to add</param>
        public void AddEvent(HistoryEvent historyEvent)
        {
            AddEvent(historyEvent, true);
        }

        ExecutionStartedEvent GetExecutionStartedEventOrThrow()
        {
            ExecutionStartedEvent? executionStartedEvent = this.ExecutionStartedEvent;
            if (executionStartedEvent == null)
            {
                throw new InvalidOperationException("An ExecutionStarted event is required.");
            }

            return executionStartedEvent;
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
            else
            {
                PastEvents.Add(historyEvent);
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
                    "The orchestration '{0}' has already seen a completed task with id {1}.",
                    this.OrchestrationInstance?.InstanceId ?? "",
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
                if (ExecutionCompletedEvent == null)
                {
                    ExecutionCompletedEvent = completedEvent;
                    orchestrationStatus = completedEvent.OrchestrationStatus;
                }
                else
                {
                    // It's not generally expected to receive multiple execution completed events for a given orchestrator, but it's possible under certain race conditions.
                    // For example: when an orchestrator is signaled to terminate at the same time as it attempts to continue-as-new.
                    var log = $"Received new {completedEvent.GetType().Name} event despite the orchestration being already in the {orchestrationStatus} state.";
                    
                    if (orchestrationStatus == OrchestrationStatus.ContinuedAsNew && completedEvent.OrchestrationStatus == OrchestrationStatus.Terminated)
                    {
                        // If the orchestration planned to continue-as-new but termination is requested, we transition to the terminated state.
                        // This is because termination should be considered to be forceful.
                        log += " Discarding previous 'ExecutionCompletedEvent' as termination is forceful.";
                        ExecutionCompletedEvent = completedEvent;
                        orchestrationStatus = completedEvent.OrchestrationStatus;
                    }
                    else
                    {
                        // otherwise, we ignore the new event.
                        log += " Discarding new 'ExecutionCompletedEvent'.";
                    }

                    LogHelper?.OrchestrationDebugTrace(this.OrchestrationInstance?.InstanceId ?? "", this.OrchestrationInstance?.ExecutionId ?? "", log);
                }
            }
            else if (historyEvent is ExecutionSuspendedEvent)
            {
                orchestrationStatus = OrchestrationStatus.Suspended;
            }
            else if (historyEvent is ExecutionResumedEvent)
            {
                if (ExecutionCompletedEvent == null)
                {
                    orchestrationStatus = OrchestrationStatus.Running;
                }
            }
        }

        /// <summary>
        /// Gets a statedump of the current list of events.
        /// </summary>
        /// <returns></returns>
        public OrchestrationRuntimeStateDump GetOrchestrationRuntimeStateDump()
        {
#if DEBUG
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
#else
            return new OrchestrationRuntimeStateDump
            {
                EventCount = Events.Count,
                NewEventsCount = NewEvents.Count,
                Events = new List<HistoryEvent>(),
                NewEvents = new List<HistoryEvent>(),
            };
#endif
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
                    Tags = taskScheduledEvent.Tags,
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
                    ClientSpanId = subOrchestrationInstanceCreatedEvent.ClientSpanId,
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