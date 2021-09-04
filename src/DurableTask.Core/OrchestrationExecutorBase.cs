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
    using System.Linq;
    using System.Threading.Tasks;
    using DurableTask.Core.Command;
    using DurableTask.Core.History;
    using DurableTask.Core.Middleware;

    public abstract class OrchestrationExecutorBase
    {
        readonly OrchestrationRuntimeState runtimeState;

        public OrchestrationExecutorBase(OrchestrationRuntimeState runtimeState)
        {
            this.runtimeState = runtimeState ?? throw new ArgumentNullException(nameof(runtimeState));
        }

        public virtual async Task<IEnumerable<OrchestratorAction>> ExecuteAsync()
        {
            try
            {
                return await this.OnExecuteAsync(this.runtimeState.PastEvents, this.runtimeState.NewEvents);
            }
            catch (Exception e)
            {
                return CreateInternalFailureActionResult(e);
            }
        }

        public virtual async Task<IEnumerable<OrchestratorAction>> ExecuteNewEventsAsync()
        {
            try
            {
                return await this.OnExecuteAsync(Enumerable.Empty<HistoryEvent>(), this.runtimeState.NewEvents);
            }
            catch (Exception e)
            {
                return CreateInternalFailureActionResult(e);
            }
        }

        static IEnumerable<OrchestratorAction> CreateInternalFailureActionResult(Exception e)
        {
            yield return new OrchestrationCompleteOrchestratorAction
            {
                Id = -1,
                OrchestrationStatus = OrchestrationStatus.Failed,
                Result = e.Message,
                Details = $"An internal failure occurred while trying to execute the orchestration: {e}",
            };
        }

        protected abstract Task<IEnumerable<OrchestratorAction>> OnExecuteAsync(IEnumerable<HistoryEvent> pastEvents, IEnumerable<HistoryEvent> newEvents);

        protected internal virtual DispatchMiddlewareContext CreateDispatchContext()
        {
            return new DispatchMiddlewareContext();
        }
    }
}
