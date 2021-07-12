using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.AzureStorage.Storage
{
    class TableQuery
    {
        public string[] SelectColumns { get; internal set; }
        public string FilterString { get; internal set; }

        internal TableQuery Where(string v)
        {
            throw new NotImplementedException();
        }

        internal void Select(IList<string> projectionColumns)
        {
            throw new NotImplementedException();
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
