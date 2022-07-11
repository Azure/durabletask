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
    using DurableTask.AzureStorage.Entities;
    using DurableTask.Core;

    /// <summary>
    /// OrchestrationInstanceStatusQueryBuilder is a builder to create a StorageTable Query
    /// </summary>
    public sealed class OrchestrationInstanceStatusQueryCondition
    {
        static readonly string[] ColumnNames = typeof(OrchestrationInstanceStatusEntity).GetProperties()
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
        /// Get the corresponding OData filter.
        /// </summary>
        /// <returns></returns>
        public (string? Filter, IEnumerable<string>? Select) ToOData()
        {
            if (!((this.RuntimeStatus == null || !this.RuntimeStatus.Any()) && 
                this.CreatedTimeFrom == default(DateTime) && 
                this.CreatedTimeTo == default(DateTime) &&
                this.TaskHubNames == null &&
                this.InstanceIdPrefix == null &&
                this.InstanceId == null))
            {
                IEnumerable<string>? select = null;
                if (!this.FetchInput || !this.FetchOutput)
                {
                    var columns = new HashSet<string>(ColumnNames);
                    if (!this.FetchInput)
                    {
                        columns.Remove(nameof(OrchestrationInstanceStatusEntity.Input));
                    }

                    if (!this.FetchOutput)
                    {
                        columns.Remove(nameof(OrchestrationInstanceStatusEntity.Output));
                    }

                    select = columns;
                }

                return (this.GetODataFilter(), select);
            }

            return default;
        }

        string? GetODataFilter()
        {
            var conditions = new List<string>();
            if (this.CreatedTimeFrom > DateTime.MinValue)
            {
                conditions.Add($"{nameof(OrchestrationInstanceStatusEntity.CreatedTime)} ge datetime'{this.CreatedTimeFrom:O}'");
            }

            if (this.CreatedTimeTo != default(DateTime) && this.CreatedTimeTo < DateTime.MaxValue)
            {
                conditions.Add($"{nameof(OrchestrationInstanceStatusEntity.CreatedTime)} le datetime'{this.CreatedTimeTo:O}'");
            }

            if (this.RuntimeStatus != null && this.RuntimeStatus.Any())
            {
                conditions.Add($"({string.Join(" or ", this.RuntimeStatus.Select(x => $"{nameof(OrchestrationInstanceStatusEntity.RuntimeStatus)} eq '{x:G}'"))})");
            }

            if (this.TaskHubNames != null && this.TaskHubNames.Any())
            {
                conditions.Add($"({string.Join(" or ", this.TaskHubNames.Select(x => $"{nameof(OrchestrationInstanceStatusEntity.TaskHubName)} eq '{x}'"))})");
            }

            if (!string.IsNullOrEmpty(this.InstanceIdPrefix))
            {
                string sanitizedPrefix = KeySanitation.EscapePartitionKey(this.InstanceIdPrefix);
                int length = sanitizedPrefix.Length - 1;
                char incrementedLastChar = (char)(sanitizedPrefix[length] + 1);

                string greaterThanPrefix = sanitizedPrefix.Substring(0, length) + incrementedLastChar;

                conditions.Add($"{nameof(OrchestrationInstanceStatusEntity.PartitionKey)} ge '{sanitizedPrefix}'");
                conditions.Add($"{nameof(OrchestrationInstanceStatusEntity.PartitionKey)} lt '{greaterThanPrefix}'");
            }

            if (this.InstanceId != null)
            {
                string sanitizedInstanceId = KeySanitation.EscapePartitionKey(this.InstanceId);
                conditions.Add($"{nameof(OrchestrationInstanceStatusEntity.PartitionKey)} eq '{sanitizedInstanceId}'");
            }

            return conditions.Count == 0 ? null : string.Join(" and ", conditions);
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
