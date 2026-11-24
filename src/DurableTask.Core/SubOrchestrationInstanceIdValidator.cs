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

    internal static class SubOrchestrationInstanceIdValidator
    {
        internal static OrchestrationCompleteOrchestratorAction? GetFailure(
            string parentInstanceId,
            OrchestrationRuntimeState runtimeState,
            IEnumerable<OrchestratorAction> decisions)
        {
            SubOrchestrationInstanceIdIndex? pendingInstances = null;
            Dictionary<string, int>? batchInstances = null;
            foreach (OrchestratorAction decision in decisions)
            {
                if (decision is not CreateSubOrchestrationAction action
                    || action.InstanceId == null
                    || OrchestrationTags.IsTaggedAsFireAndForget(action.Tags))
                {
                    continue;
                }

                pendingInstances ??= runtimeState.GetSubOrchestrationInstanceIdIndex();
                if (pendingInstances.TryGetPendingTaskId(action.InstanceId, out int priorTaskId)
                    || (batchInstances != null && batchInstances.TryGetValue(action.InstanceId, out priorTaskId)))
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

                // Proposed actions are not accepted history: a rejected or split batch may never send them.
                batchInstances ??= new Dictionary<string, int>(StringComparer.Ordinal);
                batchInstances.Add(action.InstanceId, action.Id);
            }

            return null;
        }
    }
}
