using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.AzureStorage.Storage
{
    class TableQuery
    {
        public TableQuery()
        {
            this.StorageTableQuery = new Microsoft.WindowsAzure.Storage.Table.TableQuery();
        }

        private TableQuery(Microsoft.WindowsAzure.Storage.Table.TableQuery storageQuery)
        {
            this.StorageTableQuery = storageQuery;
        }

        public Microsoft.WindowsAzure.Storage.Table.TableQuery StorageTableQuery { get; }

        public string[] SelectColumns { get; internal set; }
        public string FilterString { get; internal set; }

        public TableQuery Select(IList<string> projectionColumns)
        {
            return new TableQuery(this.StorageTableQuery.Select(projectionColumns));
        }

        public TableQuery Where(string v)
        {
            return new TableQuery(this.StorageTableQuery.Where(v));
        }

        internal static object GenerateFilterCondition(string rowKeyProperty, object equal, string empty)
        {
            throw new NotImplementedException();
        }

        internal static string GenerateFilterConditionForDate(string v, object greaterThanOrEqual, DateTimeOffset dateTimeOffset)
        {
            throw new NotImplementedException();
        }

        internal static object CombineFilters(object a, object tableOperator, object b)
        {
            throw new NotImplementedException();
        }
    }
}
