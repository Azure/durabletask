// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;

namespace DurableTask.Core.Query
{
    /// <summary>
    /// Query condition for searching the status of orchestration instances.
    /// </summary>
    public class OrchestrationStatusQueryCondition
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="OrchestrationStatusQueryCondition"/> class.
        /// </summary>
        public OrchestrationStatusQueryCondition() { }

        //internal OrchestrationStatusQueryCondition(EntityQuery entityQuery)
        //{
        //    this.CreatedTimeFrom = entityQuery.LastOperationFrom;
        //    this.CreatedTimeTo = entityQuery.LastOperationTo;
        //    this.PageSize = entityQuery.PageSize;
        //    this.ContinuationToken = entityQuery.ContinuationToken;
        //    this.ShowInput = entityQuery.FetchState;

        //    if (!string.IsNullOrEmpty(entityQuery.EntityName))
        //    {
        //        this.InstanceIdPrefix = EntityId.GetSchedulerIdPrefixFromEntityName(entityQuery.EntityName);
        //    }
        //    else
        //    {
        //        this.InstanceIdPrefix = "@";
        //    }
        //}

        /// <summary>
        /// Return orchestration instances which matches the runtimeStatus.
        /// </summary>
        public IEnumerable<OrchestrationStatus> RuntimeStatus { get; set; }

        /// <summary>
        /// Return orchestration instances which were created after this DateTime.
        /// </summary>
        public DateTime CreatedTimeFrom { get; set; }

        /// <summary>
        /// Return orchestration instances which were created before this DateTime.
        /// </summary>
        public DateTime CreatedTimeTo { get; set; }

        /// <summary>
        /// Return orchestration instances which matches the TaskHubNames.
        /// </summary>
        public IEnumerable<string> TaskHubNames { get; set; }

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
        public string ContinuationToken { get; set; }

        /// <summary>
        /// Return orchestration instances that have this instance id prefix.
        /// </summary>
        public string InstanceIdPrefix { get; set; }

        /// <summary>
        /// Determines whether the query will include the input of the orchestration.
        /// </summary>
        public bool ShowInput { get; set; } = true;
    }
}