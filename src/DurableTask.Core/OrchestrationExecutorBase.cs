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
    using DurableTask.Core.History;
    using DurableTask.Core.Middleware;

    public abstract class OrchestrationExecutorBase
    {
        readonly OrchestrationRuntimeState runtimeState;

        public OrchestrationExecutorBase(OrchestrationRuntimeState runtimeState)
        {
            this.runtimeState = runtimeState ?? throw new ArgumentNullException(nameof(runtimeState));
        }

        public virtual Task<OrchestratorExecutionResult> ExecuteAsync()
        {
            // TODO: Error handling that can distinguish between retriable and non-retriable failures
            return this.OnExecuteAsync(this.runtimeState.PastEvents, this.runtimeState.NewEvents);
        }

        public virtual Task<OrchestratorExecutionResult> ExecuteNewEventsAsync()
        {
            // TODO: Error handling that can distinguish between retriable and non-retriable failures
            return this.OnExecuteAsync(Enumerable.Empty<HistoryEvent>(), this.runtimeState.NewEvents);
        }

        protected abstract Task<OrchestratorExecutionResult> OnExecuteAsync(IEnumerable<HistoryEvent> pastEvents, IEnumerable<HistoryEvent> newEvents);

        protected internal virtual DispatchMiddlewareContext CreateDispatchContext()
        {
            return new DispatchMiddlewareContext();
        }
    }
}
