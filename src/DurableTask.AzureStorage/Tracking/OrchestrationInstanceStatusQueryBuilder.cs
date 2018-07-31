using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Text;
using System.Xml.Linq;

namespace DurableTask.AzureStorage.Tracking
{
    /// <summary>
    /// OrchestrationInstanceStatusQueryBuilder is a builder to create a StorageTable Query
    /// </summary>
    public class OrchestrationInstanceStatusQueryBuilder
    {
        private string RuntimeStatus { get; set; }
        private DateTime CreatedTimeFrom { get; set; }
        private DateTime CreatedTimeTo { get; set; }
        private DateTime LastUpdatedTimeFrom { get; set; }
        private DateTime LastUpdatedTimeTo { get; set; }

        /// <summary>
        /// AddRuntimeStatus add runtimeStatus as a query parameter 
        /// </summary>
        /// <param name="runtimeStatus">RuntimeStatus of <see cref="OrchestrationInstanceStatus"/></param>
        /// <returns>builder of object</returns>
        public OrchestrationInstanceStatusQueryBuilder AddRuntimeStatus(string runtimeStatus)
        {
            this.RuntimeStatus = runtimeStatus;
            return this;
        }

        /// <summary>
        /// AddCreatedTime add CreatedTime as a query parameter
        /// In case you want to skip the createdTime, please specify default(DateTime)
        /// </summary>
        /// <param name="createdTimeFrom">CreatedTime from</param>
        /// <param name="createdTimeTo">CreatedTime to</param>
        /// <returns></returns>
        public OrchestrationInstanceStatusQueryBuilder AddCreatedTime(DateTime createdTimeFrom, DateTime createdTimeTo)
        {
            this.CreatedTimeFrom = createdTimeFrom;
            this.CreatedTimeTo = createdTimeTo;
            return this;
        }

        /// <summary>
        /// AddLastUpdatedTime add LastUpdatedTime as a query parameter
        /// </summary>
        /// <param name="lastUpdatedTimeFrom"></param>
        /// <param name="lastUpdatedTimeTo"></param>
        /// <returns></returns>
        public OrchestrationInstanceStatusQueryBuilder AddLastUpdatedTime(DateTime lastUpdatedTimeFrom, DateTime lastUpdatedTimeTo)
        {
            this.LastUpdatedTimeFrom = lastUpdatedTimeFrom;
            this.LastUpdatedTimeTo = lastUpdatedTimeTo;
            return this;
        }


        /// <summary>
        /// Build returns query object.
        /// </summary>
        /// <returns></returns>
        public TableQuery<T> Build<T>() 
            where T : TableEntity, new()
        {
            var query = new TableQuery<T>()
                .Where(
                    GetConditions()
                    );
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

            if (default(DateTime) != this.LastUpdatedTimeFrom)
            {
                conditions.Add(TableQuery.GenerateFilterConditionForDate("LastUpdatedTime", QueryComparisons.GreaterThanOrEqual, new DateTimeOffset(this.LastUpdatedTimeFrom)));
            }

            if (default(DateTime) != this.LastUpdatedTimeTo)
            {
                conditions.Add(TableQuery.GenerateFilterConditionForDate("LastUpdatedTime", QueryComparisons.LessThanOrEqual, new DateTimeOffset(this.LastUpdatedTimeTo)));
            }

            if (!string.IsNullOrEmpty(this.RuntimeStatus))
            {
                conditions.Add(TableQuery.GenerateFilterCondition("RuntimeStatus", QueryComparisons.Equal, this.RuntimeStatus));
            }

            if (conditions.Count == 1)
            {
                return conditions[0];
            }
            else
            {
                string lastCondition = null;
                foreach (var condition in conditions)
                {
                    if (string.IsNullOrEmpty(lastCondition))
                    {
                        lastCondition = condition;
                        continue;
                    }

                    lastCondition = TableQuery.CombineFilters(lastCondition, TableOperators.And, condition);

                }
                return lastCondition;
            }

        }


    }
}
