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

namespace DurableTask.AzureStorage.Tracking
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using DurableTask.Core;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// OrchestrationInstanceStatusQueryBuilder is a builder to create a StorageTable Query
    /// </summary>
    public class OrchestrationInstanceStatusQueryCondition
    {
        /// <summary>
        /// RuntimeStatus
        /// </summary>
        public IEnumerable<OrchestrationStatus> RuntimeStatus { get; set; }

        /// <summary>
        /// CreatedTimeFrom
        /// </summary>
        public DateTime CreatedTimeFrom { get; set; }

        /// <summary>
        /// CreatedTimeTo
        /// </summary>
        public DateTime CreatedTimeTo { get; set; }

        /// <summary>
        /// Collection of TaskHub name
        /// </summary>
        public IEnumerable<string> TaskHubNames { get; set; }

        /// <summary>
        /// Get the TableQuery object
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public TableQuery<T> ToTableQuery<T>()
            where T : TableEntity, new()
        {
            var query = new TableQuery<T>();
            if (!((this.RuntimeStatus == null || (!this.RuntimeStatus.Any())) && 
                this.CreatedTimeFrom == default(DateTime) && 
                this.CreatedTimeTo == default(DateTime) &&
                this.TaskHubNames == null))
            {
                query.Where(this.GetConditions());
            }

            return query;
        }

        string GetConditions()
        {
            var conditions = new List<string>();

            if (default(DateTime) != this.CreatedTimeFrom)
            {
                conditions.Add(TableQuery.GenerateFilterConditionForDate("CreatedTime", QueryComparisons.GreaterThanOrEqual, new DateTimeOffset(this.CreatedTimeFrom)));
            }

            if (default(DateTime) != this.CreatedTimeTo)
            {
                conditions.Add(TableQuery.GenerateFilterConditionForDate("CreatedTime", QueryComparisons.LessThanOrEqual, new DateTimeOffset(this.CreatedTimeTo)));
            }

            if (this.RuntimeStatus != null && this.RuntimeStatus.Any())
            {
                string runtimeCondition = this.RuntimeStatus.Select(x => TableQuery.GenerateFilterCondition("RuntimeStatus", QueryComparisons.Equal, x.ToString()))
                                    .Aggregate((a, b) => TableQuery.CombineFilters(a, TableOperators.Or, b));
                if (runtimeCondition.Count() != 0)
                {
                    conditions.Add(runtimeCondition);
                }
            }

            if (this.TaskHubNames != null)
            {
                string taskHubCondition = this.TaskHubNames.Select(x => TableQuery.GenerateFilterCondition("TaskHubName", QueryComparisons.Equal, x.ToString()))
                    .Aggregate((a, b) => TableQuery.CombineFilters(a, TableOperators.Or, b));
                if (taskHubCondition.Count() != 0)
                {
                    conditions.Add(taskHubCondition);
                }
            }

            return conditions.Count == 1 ? 
                conditions[0] : 
                conditions.Aggregate((a, b) => TableQuery.CombineFilters(a, TableOperators.And, b));
        }

        /// <summary>
        /// Parse is a factory method of the OrchestrationInstanceStatusConditionQuery
        /// </summary>
        /// <param name="createdTimeFrom">CreatedTimeFrom</param>
        /// <param name="createdTimeTo">CreatedTimeTo</param>
        /// <param name="runtimeStatus">RuntimeStatus</param>
        /// <returns></returns>
        public static OrchestrationInstanceStatusQueryCondition Parse(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus)
        {
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                CreatedTimeFrom = createdTimeFrom,
                CreatedTimeTo = createdTimeTo ?? default(DateTime),
                RuntimeStatus = runtimeStatus
            };
            return condition;
        }
    }
}
