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

namespace DurableTask.AzureStorage
{
    /// <summary>
    /// Specifies which kinds of work a single worker instance dispatches, enabling orchestrations
    /// and activities to be scaled independently across separate worker deployments on the same task hub.
    /// </summary>
    /// <remarks>
    /// This is a single, self-contained value so it can be surfaced from a single environment variable
    /// or configuration setting without any code changes.
    /// </remarks>
    public enum WorkerDispatchMode
    {
        /// <summary>
        /// The worker dispatches both orchestrations (including entities) and activities.
        /// This is the default and matches the historical behavior.
        /// </summary>
        Both = 0,

        /// <summary>
        /// The worker leases control-queue partitions and dispatches only orchestrations and entities.
        /// It does not dequeue the work-item queue, so it runs no activities.
        /// </summary>
        Orchestrator = 1,

        /// <summary>
        /// The worker dispatches only activities from the work-item queue.
        /// It does not lease any control-queue partition, so it runs no orchestrations or entities.
        /// </summary>
        Activity = 2,
    }
}
