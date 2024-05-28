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

    /// <summary>
    /// OrchestrationInstanceStatusQueryBuilder is a builder to create a StorageTable Query
    /// </summary>
    public sealed class OrchestrationInstanceStatusQueryCondition
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
        /// Get the corresponding OData filter.
        /// </summary>
        /// <returns></returns>
        internal ODataCondition ToOData()
        {
            if (!((this.RuntimeStatus == null || !this.RuntimeStatus.Any()) &&
                this.CreatedTimeFrom == default(DateTime) &&
                this.CreatedTimeTo == default(DateTime) &&
                this.TaskHubNames == null &&
                this.InstanceIdPrefix == null &&
                this.InstanceId == null &&
                !this.ExcludeEntities))
            {
                IEnumerable<string>? select = null;
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

                    select = columns;
                }

                return new ODataCondition(select, this.GetODataFilter());
            }

            return default;
        }

        string? GetODataFilter()
        {
            var conditions = new List<string>();
            if (this.CreatedTimeFrom > DateTime.MinValue)
            {
                conditions.Add($"{nameof(OrchestrationInstanceStatus.CreatedTime)} ge datetime'{this.CreatedTimeFrom:O}'");
            }

            if (this.CreatedTimeTo != default(DateTime) && this.CreatedTimeTo < DateTime.MaxValue)
            {
                conditions.Add($"{nameof(OrchestrationInstanceStatus.CreatedTime)} le datetime'{this.CreatedTimeTo:O}'");
            }

            if (this.RuntimeStatus != null && this.RuntimeStatus.Any())
            {
                conditions.Add($"{string.Join(" or ", this.RuntimeStatus.Select(x => $"{nameof(OrchestrationInstanceStatus.RuntimeStatus)} eq '{x:G}'"))}");
            }

            if (this.TaskHubNames != null && this.TaskHubNames.Any())
            {
                conditions.Add($"{string.Join(" or ", this.TaskHubNames.Select(x => $"TaskHubName eq '{x}'"))}");
            }

            if (!string.IsNullOrEmpty(this.InstanceIdPrefix))
            {
                string sanitizedPrefix = KeySanitation.EscapePartitionKey(this.InstanceIdPrefix);
                int length = sanitizedPrefix.Length - 1;
                char incrementedLastChar = (char)(sanitizedPrefix[length] + 1);

                string greaterThanPrefix = sanitizedPrefix.Substring(0, length) + incrementedLastChar;

                conditions.Add($"{nameof(OrchestrationInstanceStatus.PartitionKey)} ge '{sanitizedPrefix}'");
                conditions.Add($"{nameof(OrchestrationInstanceStatus.PartitionKey)} lt '{greaterThanPrefix}'");
            }
            else if (this.ExcludeEntities)
            {
                conditions.Add($"{nameof(OrchestrationInstanceStatus.PartitionKey)} lt '@' or {nameof(OrchestrationInstanceStatus.PartitionKey)} ge 'A'");
            }

            if (this.InstanceId != null)
            {
                string sanitizedInstanceId = KeySanitation.EscapePartitionKey(this.InstanceId);
                conditions.Add($"{nameof(OrchestrationInstanceStatus.PartitionKey)} eq '{sanitizedInstanceId}'");
            }

            return conditions.Count switch
            {
                0 => null,
                1 => conditions[0],
                _ => string.Join(" and ", conditions.Select(c => $"({c})")),
            };
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
