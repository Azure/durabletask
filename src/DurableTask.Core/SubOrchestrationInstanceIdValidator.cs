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
    using DurableTask.Core.Command;
    using DurableTask.Core.History;

    internal static class SubOrchestrationInstanceIdValidator
    {
        internal static OrchestrationCompleteOrchestratorAction? GetFailure(
            string parentInstanceId,
            IEnumerable<HistoryEvent> history,
            IEnumerable<OrchestratorAction> decisions)
        {
            Dictionary<string, int>? pendingInstances = null;
            foreach (OrchestratorAction decision in decisions)
            {
                if (decision is not CreateSubOrchestrationAction action
                    || action.InstanceId == null
                    || OrchestrationTags.IsTaggedAsFireAndForget(action.Tags))
                {
                    continue;
                }

                // Most episodes do not start awaited children, so only scan history when needed.
                pendingInstances ??= GetPendingInstances(history);
                if (pendingInstances.TryGetValue(action.InstanceId, out int priorTaskId))
                {
                    string message = $"Orchestration '{parentInstanceId}' attempted to start sub-orchestration "
                        + $"'{action.InstanceId}' with task ID {action.Id}, but task ID {priorTaskId} is still pending "
                        + "with the same instance ID. Use distinct instance IDs for concurrent sub-orchestrations, "
                        + "omit the instance ID to generate one automatically, or await completion before reusing an ID.";

                    return new OrchestrationCompleteOrchestratorAction
                    {
                        Id = action.Id,
                        OrchestrationStatus = OrchestrationStatus.Failed,
                        Result = message,
                        FailureDetails = new FailureDetails("DuplicateSubOrchestrationInstanceId", message, null, null, true),
                    };
                }

                pendingInstances.Add(action.InstanceId, action.Id);
            }

            return null;
        }

        static Dictionary<string, int> GetPendingInstances(IEnumerable<HistoryEvent> history)
        {
            var pendingTasks = new Dictionary<int, string>();
            foreach (HistoryEvent historyEvent in history)
            {
                switch (historyEvent)
                {
                    case SubOrchestrationInstanceCreatedEvent created
                        when created.InstanceId != null && !OrchestrationTags.IsTaggedAsFireAndForget(created.Tags):
                        pendingTasks[created.EventId] = created.InstanceId;
                        break;
                    case SubOrchestrationInstanceCompletedEvent completed:
                        pendingTasks.Remove(completed.TaskScheduledId);
                        break;
                    case SubOrchestrationInstanceFailedEvent failed:
                        pendingTasks.Remove(failed.TaskScheduledId);
                        break;
                }
            }

            // Legacy history may already contain duplicate IDs. Only a new conflicting start is rejected.
            var pendingInstances = new Dictionary<string, int>(StringComparer.Ordinal);
            foreach (KeyValuePair<int, string> pendingTask in pendingTasks)
            {
                pendingInstances[pendingTask.Value] = pendingTask.Key;
            }

            return pendingInstances;
        }
    }
}
