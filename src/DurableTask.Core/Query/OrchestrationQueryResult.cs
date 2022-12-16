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
namespace DurableTask.Core.Query
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// The status of all orchestration instances with paging for a given query.
    /// </summary>
    public class OrchestrationQueryResult
    {
        /// <summary>
        /// Constructor of OrchestrationQueryResult Class.
        /// </summary>
        /// <param name="orchestrationState">A collection of orchestration instance status values.</param>
        /// <param name="continuationToken">A server-generated continuation token or <c>null</c> if there are no further continuations.</param>
        public OrchestrationQueryResult(IReadOnlyCollection<OrchestrationState> orchestrationState, string? continuationToken)
        {
            this.OrchestrationState = orchestrationState ?? throw new ArgumentNullException(nameof(orchestrationState));
            this.ContinuationToken = continuationToken;
        }
        /// <summary>
        /// Gets a collection of statuses of orchestration instances matching the query description.
        /// </summary>
        /// <value>A collection of orchestration instance status values.</value>
        public IReadOnlyCollection<OrchestrationState> OrchestrationState { get; }

        /// <summary>
        /// Gets token that can be used to resume the query with data not already returned by this query.
        /// </summary>
        /// <value>A server-generated continuation token or <c>null</c> if there are no further continuations.</value>
        public string? ContinuationToken { get; }
    }
}
