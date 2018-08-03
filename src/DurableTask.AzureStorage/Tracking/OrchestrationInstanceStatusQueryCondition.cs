using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;

namespace DurableTask.AzureStorage.Tracking
{
    /// <summary>
    /// OrchestrationInstanceStatusQueryBuilder is a builder to create a StorageTable Query
    /// </summary>
    public class OrchestrationInstanceStatusQueryCondition
    {
        /// <summary>
        /// RuntimeStatus
        /// </summary>
        public string RuntimeStatus { get; set; }
        /// <summary>
        /// CreatedTimeFrom. Greater than this time
        /// </summary>
        public DateTime CreatedTimeFrom { get; set; }
        /// <summary>
        /// CreatedTimeTo. Less than this time
        /// </summary>
        public DateTime CreatedTimeTo { get; set; }

        /// <summary>
        /// Build returns query object.
        /// </summary>
        /// <returns></returns>
        public TableQuery<T> ToTableQuery<T>() 
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
                return conditions.Aggregate((a, b) => TableQuery.CombineFilters(a, TableOperators.And, b));
            }

        }

    }
}
