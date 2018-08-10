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
    using DurableTask.Core;
    using Microsoft.WindowsAzure.Storage.Table;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Xml.Linq;

    /// <summary>
    /// OrchestrationInstanceStatusQueryBuilder is a builder to create a StorageTable Query
    /// </summary>
    internal class OrchestrationInstanceStatusQueryCondition
    {
        public IEnumerable<string> RuntimeStatus { get; set; }

        public DateTime CreatedTimeFrom { get; set; }

        public DateTime CreatedTimeTo { get; set; }

        public TableQuery<T> ToTableQuery<T>()
            where T : TableEntity, new()
        {
            var query = new TableQuery<T>();
            if (!((RuntimeStatus == null || RuntimeStatus.Count() == 0) && CreatedTimeFrom == default(DateTime) && CreatedTimeTo == default(DateTime)))
            {
                query.Where(
                    GetConditions()
                    );
            }
            return query;
        }

        private string GetConditions()
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

            if (this.RuntimeStatus != null && this.RuntimeStatus.Count() != 0)
            {
                var runtimeCondition = this.RuntimeStatus.Select(x => TableQuery.GenerateFilterCondition("RuntimeStatus", QueryComparisons.Equal, x))
                                    .Aggregate((a, b) => TableQuery.CombineFilters(a, TableOperators.Or, b));
                if (runtimeCondition.Count() != 0)
                {
                    conditions.Add(runtimeCondition);
                }
            }

            if (conditions.Count == 1)
            {
                return conditions[0];
            }
            else
            {
                return conditions.Aggregate((a, b) => TableQuery.CombineFilters(a, TableOperators.And, b));
            }

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
            var condition = new OrchestrationInstanceStatusQueryCondition();
            condition.CreatedTimeFrom = createdTimeFrom;
            condition.CreatedTimeTo = (createdTimeTo != null) ? (DateTime)createdTimeTo : default(DateTime);
            condition.RuntimeStatus = runtimeStatus.Select(x => x.ToString());
            return condition;
        }
    }
}
