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
        static readonly string[] ColumnNames = typeof(OrchestrationInstanceStatus).GetProperties()
            .Select(prop => prop.Name)
            .ToArray();

        /// <summary>
        /// RuntimeStatus
        /// </summary>
        public IEnumerable<OrchestrationStatus>? RuntimeStatus { get; set; }

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
        public IEnumerable<string>? TaskHubNames { get; set; }

        /// <summary>
        /// InstanceIdPrefix
        /// </summary>
        public string? InstanceIdPrefix { get; set; }

        /// <summary>
        /// InstanceId
        /// </summary>
        public string? InstanceId { get; set; }

        /// <summary>
        /// If true, the input will be returned with the results. The default value is true.
        /// </summary>
        public bool FetchInput { get; set; } = true;

        /// <summary>
        /// If true, the output will be returned with the results. The default value is true.
        /// </summary>
        public bool FetchOutput { get; set; } = true;

        /// <summary>
        /// Whether to exclude entities from the results.
        /// </summary>
        public bool ExcludeEntities { get; set; } = false;

        /// <summary>
        /// Get the TableQuery object
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public TableQuery<T> ToTableQuery<T>()
            where T : ITableEntity, new()
        {
            var query = new TableQuery<T>();
            if (!((this.RuntimeStatus == null || !this.RuntimeStatus.Any()) && 
                this.CreatedTimeFrom == default(DateTime) && 
                this.CreatedTimeTo == default(DateTime) &&
                this.TaskHubNames == null &&
                this.InstanceIdPrefix == null &&
                this.InstanceId == null &&
                !this.ExcludeEntities))
            {
                if (!this.FetchInput || !this.FetchOutput)
                {
                    var columns = new HashSet<string>(ColumnNames);
                    if (!this.FetchInput)
                    {
                        columns.Remove(nameof(OrchestrationInstanceStatus.Input));
                    }

                    if (!this.FetchOutput)
                    {
                        columns.Remove(nameof(OrchestrationInstanceStatus.Output));
                    }

                    query.Select(columns.ToList());
                }

                string conditions = this.GetConditions();
                if (!string.IsNullOrEmpty(conditions))
                {
                    query.Where(conditions);
                }
            }

            return query;
        }

        string GetConditions()
        {
            var conditions = new List<string>();

            if (this.CreatedTimeFrom > DateTime.MinValue)
            {
                conditions.Add(TableQuery.GenerateFilterConditionForDate("CreatedTime", QueryComparisons.GreaterThanOrEqual, new DateTimeOffset(this.CreatedTimeFrom)));
            }

            if (this.CreatedTimeTo != default(DateTime) && this.CreatedTimeTo < DateTime.MaxValue)
            {
                conditions.Add(TableQuery.GenerateFilterConditionForDate("CreatedTime", QueryComparisons.LessThanOrEqual, new DateTimeOffset(this.CreatedTimeTo)));
            }

            if (this.RuntimeStatus != null && this.RuntimeStatus.Any())
            {
                string runtimeCondition = this.RuntimeStatus
                    .Select(x => TableQuery.GenerateFilterCondition("RuntimeStatus", QueryComparisons.Equal, x.ToString()))
                    .Aggregate((a, b) => TableQuery.CombineFilters(a, TableOperators.Or, b));
                if (runtimeCondition.Length > 0)
                {
                    conditions.Add(runtimeCondition);
                }
            }

            if (this.TaskHubNames != null)
            {
                string taskHubCondition = this.TaskHubNames
                    .Select(x => TableQuery.GenerateFilterCondition("TaskHubName", QueryComparisons.Equal, x.ToString()))
                    .Aggregate((a, b) => TableQuery.CombineFilters(a, TableOperators.Or, b));
                if (taskHubCondition.Count() != 0)
                {
                    conditions.Add(taskHubCondition);
                }
            }

            if (!string.IsNullOrEmpty(this.InstanceIdPrefix))
            {
                string sanitizedPrefix = KeySanitation.EscapePartitionKey(this.InstanceIdPrefix);
                int length = sanitizedPrefix.Length - 1;
                char incrementedLastChar = (char)(sanitizedPrefix[length] + 1);

                string greaterThanPrefix = sanitizedPrefix.Substring(0, length) + incrementedLastChar;

                conditions.Add(TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThanOrEqual, sanitizedPrefix), 
                    TableOperators.And, 
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThan, greaterThanPrefix)));
            }
            else if (this.ExcludeEntities)
            {
                conditions.Add(TableQuery.CombineFilters(
                     TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThan, "@"),
                     TableOperators.Or,
                     TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThanOrEqual, "A")));
            }

            if (this.InstanceId != null)
            {
                string sanitizedInstanceId = KeySanitation.EscapePartitionKey(this.InstanceId);
                conditions.Add(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, sanitizedInstanceId));
            }

            if (conditions.Count == 0)
            {
                return string.Empty;
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
        public static OrchestrationInstanceStatusQueryCondition Parse(
            DateTime createdTimeFrom,
            DateTime? createdTimeTo,
            IEnumerable<OrchestrationStatus> runtimeStatus)
        {
            var condition = new OrchestrationInstanceStatusQueryCondition
            {
                CreatedTimeFrom = createdTimeFrom,
                CreatedTimeTo = createdTimeTo ?? default(DateTime),
                RuntimeStatus = runtimeStatus,
            };
            return condition;
        }
    }
}
