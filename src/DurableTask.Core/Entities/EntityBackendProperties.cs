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
namespace DurableTask.Core.Entities
{
    using System;

    /// <summary>
    /// Entity processing characteristics that are controlled by the backend provider, i.e. the orchestration service.
    /// </summary>
    public class EntityBackendProperties
    {
        /// <summary>
        /// The time window within which entity messages should be deduplicated and reordered.
        /// This is zero for providers that already guarantee exactly-once and ordered delivery.
        /// </summary>
        public TimeSpan EntityMessageReorderWindow { get; set; }

        /// <summary>
        /// A limit on the number of entity operations that should be processed as a single batch, or null if there is no limit.
        /// </summary>
        public int? MaxEntityOperationBatchSize { get; set; }

        /// <summary>
        /// The maximum number of entity operation batches that can be processed concurrently on a single node.
        /// </summary>
        public int MaxConcurrentTaskEntityWorkItems { get; set; }

        /// <summary>
        /// Whether the backend supports implicit deletion, i.e. setting the entity scheduler state to null implicitly deletes the storage record.
        /// </summary>
        public bool SupportsImplicitEntityDeletion { get; set; }

        /// <summary>
        /// Value of maximum durable timer delay. Used for delayed signals.
        /// </summary>
        public TimeSpan MaximumSignalDelayTime { get; set; }

        /// <summary>
        /// Computes a cap on the scheduled time of an entity signal, based on the maximum signal delay time
        /// </summary>
        /// <param name="nowUtc"></param>
        /// <param name="scheduledUtcTime"></param>
        /// <returns></returns>
        public DateTime GetCappedScheduledTime(DateTime nowUtc, DateTime scheduledUtcTime)
        {
            if ((scheduledUtcTime - nowUtc) <= this.MaximumSignalDelayTime)
            {
                return scheduledUtcTime;
            }
            else
            {
                return nowUtc + this.MaximumSignalDelayTime;
            }
        }
    }
}
