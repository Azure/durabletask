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
    using System.Collections.Generic;
    using System.Threading.Tasks;

    /// <summary>
    /// Interface allowing providers to implement extended sessions (aka "sticky sessions").
    /// </summary>
    public interface IOrchestrationSession
    {
        /// <summary>
        /// When implemented, fetches a new batch of messages for a particular work item.
        /// </summary>
        /// <remarks>
        /// Implementors of this method should feel free to block until new messages are available,
        /// or until an internal wait period has expired. In either case, <c>null</c> can be returned
        /// and the dispatcher will shut down the session.
        /// </remarks>
        Task<IList<TaskMessage>> FetchNewOrchestrationMessagesAsync(TaskOrchestrationWorkItem workItem);

        /// <summary>
        /// Updates the in-memory session info.
        /// </summary>
        void UpdateRuntimeState(OrchestrationRuntimeState runtimeState);
    }
}
