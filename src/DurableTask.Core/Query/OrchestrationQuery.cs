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
    /// Query condition for searching the status of orchestration instances.
    /// </summary>
    public class OrchestrationQuery
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="OrchestrationQuery"/> class.
        /// </summary>
        public OrchestrationQuery() { }

        /// <summary>
        /// Return orchestration instances which matches the runtimeStatus.
        /// </summary>
        public ICollection<OrchestrationStatus>? RuntimeStatus { get; set; }

        /// <summary>
        /// Return orchestration instances which were created after this DateTime.
        /// </summary>
        public DateTime? CreatedTimeFrom { get; set; }

        /// <summary>
        /// Return orchestration instances which were created before this DateTime.
        /// </summary>
        public DateTime? CreatedTimeTo { get; set; }

        /// <summary>
        /// Return orchestration instances which matches the TaskHubNames.
        /// </summary>
        public ICollection<string>? TaskHubNames { get; set; }

        /// <summary>
        /// Maximum number of records that can be returned by the request. The default value is 100.
        /// </summary>
        /// <remarks>
        /// Requests may return fewer records than the specified page size, even if there are more records.
        /// Always check the continuation token to determine whether there are more records.
        /// </remarks>
        public int PageSize { get; set; } = 100;

        /// <summary>
        /// ContinuationToken of the pager.
        /// </summary>
        public string? ContinuationToken { get; set; }

        /// <summary>
        /// Return orchestration instances that have this instance id prefix.
        /// </summary>
        public string? InstanceIdPrefix { get; set; }

        /// <summary>
        /// Determines whether the query will include the input of the orchestration.
        /// </summary>
        public bool FetchInputsAndOutputs { get; set; } = true;

        /// <summary>
        /// Whether to exclude entities from the query results. This defaults to false for compatibility with older SDKs,
        /// but is set to true by the newer SDKs.
        /// </summary>
        public bool ExcludeEntities { get; set; } = false;
    }
}