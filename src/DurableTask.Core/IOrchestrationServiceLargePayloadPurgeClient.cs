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
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Optional orchestration service client capability for purging tombstoned large payloads.
    /// </summary>
    public interface IOrchestrationServiceLargePayloadPurgeClient
    {
        /// <summary>
        /// Enables or disables large payload auto-purge for the client's task hub.
        /// </summary>
        /// <param name="enabled">Whether large payload auto-purge is enabled.</param>
        /// <param name="deadlineUtc">The operation deadline in UTC, or <see cref="DateTime.MaxValue"/> for no deadline.</param>
        /// <param name="cancellationToken">The token used to cancel the operation.</param>
        /// <returns>A task that represents the operation.</returns>
        Task SetLargePayloadAutoPurgeAsync(bool enabled, DateTime deadlineUtc, CancellationToken cancellationToken);

        /// <summary>
        /// Gets tombstoned large payloads that are ready to be purged.
        /// </summary>
        /// <param name="limit">The maximum number of tombstones to return.</param>
        /// <param name="deadlineUtc">The operation deadline in UTC, or <see cref="DateTime.MaxValue"/> for no deadline.</param>
        /// <param name="cancellationToken">The token used to cancel the operation.</param>
        /// <returns>The tombstones to process.</returns>
        Task<IReadOnlyList<LargePayloadPurgeTombstone>> GetLargePayloadsToPurgeAsync(
            int limit, DateTime deadlineUtc, CancellationToken cancellationToken);

        /// <summary>
        /// Reports the outcomes of attempts to purge tombstoned large payloads.
        /// </summary>
        /// <param name="results">The purge outcomes, including the unchanged tombstone correlation tokens.</param>
        /// <param name="deadlineUtc">The operation deadline in UTC, or <see cref="DateTime.MaxValue"/> for no deadline.</param>
        /// <param name="cancellationToken">The token used to cancel the operation.</param>
        /// <returns>A task that represents the operation.</returns>
        Task ReportLargePayloadPurgeResultsAsync(
            IReadOnlyList<LargePayloadPurgeResult> results, DateTime deadlineUtc, CancellationToken cancellationToken);
    }
}
