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
namespace DurableTask.Core.Common
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using DurableTask.Core;
    using DurableTask.Core.History;

    /// <summary>
    /// Functionality for estimating the number of bytes of memory taken up by some commonly used objects.
    /// </summary>
    public class SizeUtils
    {
        /// <summary>
        /// Returns the approximate number of bytes of memory used by the given orchestration state object.
        /// </summary>
        /// <param name="state">The object.</param>
        /// <returns>The estimated number of bytes.</returns>
        public static long GetEstimatedSize(OrchestrationState state)
        {
            long sum = 0;
            if (state != null)
            {
                sum += 120; // sum of members of constant size
                sum += 2 * ((state.Status?.Length ?? 0) + (state.Output?.Length ?? 0) + (state.Name?.Length ?? 0) + (state.Input?.Length ?? 0) + (state.Version?.Length ?? 0));
                sum += GetEstimatedSize(state.OrchestrationInstance) + GetEstimatedSize(state.ParentInstance);
            }
            return sum;
        }

        static long GetEstimatedSize(OrchestrationInstance instance)
        {
            return instance == null ? 0 : 32 + 2 * (instance.InstanceId?.Length ?? 0 + instance.ExecutionId?.Length ?? 0);
        }

        static long GetEstimatedSize(ParentInstance p)
        {
            return p == null ? 0 : 36 + 2 * (p.Name?.Length ?? 0 + p.Version?.Length ?? 0) + GetEstimatedSize(p.OrchestrationInstance);
        }

        /// <summary>
        /// Returns the approximate number of bytes of memory used by the given history event object.
        /// </summary>
        /// <param name="historyEvent">The history event object.</param>
        /// <returns>The estimated number of bytes.</returns>
        public static long GetEstimatedSize(HistoryEvent historyEvent)
        {
            long estimate = 5 * 8; // estimated size of base class
            void AddStringSize(string s) => estimate += 8 + ((s == null) ? 0 : 2 * s.Length);
            switch (historyEvent)
            {
                case ContinueAsNewEvent continueAsNewEvent:
                    AddStringSize(continueAsNewEvent.Result);
                    break;
                case EventRaisedEvent eventRaisedEvent:
                    AddStringSize(eventRaisedEvent.Input);
                    AddStringSize(eventRaisedEvent.Name);
                    break;
                case EventSentEvent eventSentEvent:
                    AddStringSize(eventSentEvent.InstanceId);
                    AddStringSize(eventSentEvent.Name);
                    AddStringSize(eventSentEvent.Input);
                    break;
                case ExecutionCompletedEvent executionCompletedEvent:
                    AddStringSize(executionCompletedEvent.Result);
                    estimate += 8;
                    break;
                case ExecutionSuspendedEvent executionSuspendedEvent:
                    AddStringSize(executionSuspendedEvent.Reason);
                    break;
                case ExecutionResumedEvent executionResumedEvent:
                    AddStringSize(executionResumedEvent.Reason);
                    break;
                case ExecutionStartedEvent executionStartedEvent:
                    AddStringSize(executionStartedEvent.Input);
                    AddStringSize(executionStartedEvent.Name);
                    AddStringSize(executionStartedEvent.OrchestrationInstance.InstanceId);
                    AddStringSize(executionStartedEvent.OrchestrationInstance.ExecutionId);
                    estimate += 8 + (executionStartedEvent.Tags == null ? 0
                        : executionStartedEvent.Tags.Select(kvp => 20 + 2 * (kvp.Key.Length + kvp.Value.Length)).Sum());
                    AddStringSize(executionStartedEvent.Version);
                    AddStringSize(executionStartedEvent.ParentInstance?.OrchestrationInstance.InstanceId);
                    AddStringSize(executionStartedEvent.ParentInstance?.OrchestrationInstance.ExecutionId);
                    estimate += 8;
                    break;
                case ExecutionTerminatedEvent executionTerminatedEvent:
                    AddStringSize(executionTerminatedEvent.Input);
                    break;
                case GenericEvent genericEvent:
                    AddStringSize(genericEvent.Data);
                    break;
                case OrchestratorCompletedEvent:
                    break;
                case OrchestratorStartedEvent:
                    break;
                case SubOrchestrationInstanceCompletedEvent subOrchestrationInstanceCompletedEvent:
                    estimate += 8;
                    AddStringSize(subOrchestrationInstanceCompletedEvent.Result);
                    break;
                case SubOrchestrationInstanceCreatedEvent subOrchestrationInstanceCreatedEvent:
                    AddStringSize(subOrchestrationInstanceCreatedEvent.Input);
                    AddStringSize(subOrchestrationInstanceCreatedEvent.Input);
                    AddStringSize(subOrchestrationInstanceCreatedEvent.Name);
                    AddStringSize(subOrchestrationInstanceCreatedEvent.Version);
                    break;
                case SubOrchestrationInstanceFailedEvent subOrchestrationInstanceFailedEvent:
                    estimate += 8;
                    AddStringSize(subOrchestrationInstanceFailedEvent.Reason);
                    AddStringSize(subOrchestrationInstanceFailedEvent.Details);
                    break;
                case TaskCompletedEvent taskCompletedEvent:
                    estimate += 8;
                    AddStringSize(taskCompletedEvent.Result);
                    break;
                case TaskFailedEvent taskFailedEvent:
                    estimate += 8;
                    AddStringSize(taskFailedEvent.Reason);
                    AddStringSize(taskFailedEvent.Details);
                    break;
                case TaskScheduledEvent taskScheduledEvent:
                    AddStringSize(taskScheduledEvent.Input);
                    AddStringSize(taskScheduledEvent.Name);
                    AddStringSize(taskScheduledEvent.Version);
                    break;
                case TimerCreatedEvent timerCreatedEvent:
                    estimate += 8;
                    break;
                case TimerFiredEvent timerFiredEvent:
                    estimate += 16;
                    break;
                default:
                    break;
            }
            return estimate;
        }
    }
}
