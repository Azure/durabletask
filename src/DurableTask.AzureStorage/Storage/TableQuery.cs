using System;
using System.Collections.Generic;

namespace DurableTask.AzureStorage.Storage
{
    class TableQuery
    {
        private Microsoft.WindowsAzure.Storage.Table.TableQuery storageTableQuery;

        public TableQuery()
        {
            this.storageTableQuery = new Microsoft.WindowsAzure.Storage.Table.TableQuery();
        }

        public TableQuery Select(IList<string> projectionColumns)
        {
            this.storageTableQuery = this.storageTableQuery.Select(projectionColumns);
            return this;
        }

        public TableQuery Where(string v)
        {
            this.storageTableQuery = this.storageTableQuery.Where(v);
            return this;
        }

        public static string GenerateFilterCondition(string propertyName, QueryComparisons comparison, string value)
        {
            return Microsoft.WindowsAzure.Storage.Table.TableQuery.GenerateFilterCondition(propertyName, comparison.ToString(), value);
        }

        public static string GenerateFilterCondition(string propertyName, QueryComparisons comparison, DateTimeOffset dateTimeOffset)
        {
            return Microsoft.WindowsAzure.Storage.Table.TableQuery.GenerateFilterConditionForDate(propertyName, comparison.ToString(), dateTimeOffset);
        }

        public static string CombineFilters(string filterA, TableOperators tableOperator, string filterB)
        {
            return Microsoft.WindowsAzure.Storage.Table.TableQuery.CombineFilters(filterA, tableOperator.ToString(), filterB);
        }
    }
}
