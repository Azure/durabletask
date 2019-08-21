using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;

namespace DurableTask.SqlServer.Internal
{
    /// <summary>
    /// Intended for internal use only; not all edge cases are handled, but these extension methods will work correctly for the queries defined in this assembly and results in more readable code.
    /// </summary>
    internal static class DbCommandHelper
    {
        internal readonly static IDictionary<string, object> EmptyParameters = new Dictionary<string, object>();

        internal static void AddStatement(this DbCommand source, string sql, IDictionary<string, object> parameters = null)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));

            //replace each parameter in the sql statement with auto-generated names
            //add parameters using new auto-generated names
            foreach (var parameter in parameters ?? EmptyParameters)
            {
                var newName = Guid.NewGuid().ToString("N");
                sql = sql.Replace("@" + parameter.Key, "@" + newName);
                source.AddParameter(newName, parameter.Value);
            }

            //add newline to ensure commands have some white-space between them; added two new lines for readability
            if (!string.IsNullOrWhiteSpace(source.CommandText))
                source.CommandText += Environment.NewLine + Environment.NewLine;

            source.CommandText += sql;
            return;
        }

        internal static void AddStatement(this DbCommand source, string sql, object parameters)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));

            var dictionary = new Dictionary<string, object>();

            //convert object to dictionary
            foreach(PropertyDescriptor descriptor in TypeDescriptor.GetProperties(parameters))
            {
                dictionary.Add(descriptor.Name, descriptor.GetValue(parameters));
            }

            source.AddStatement(sql, dictionary);
        }

        internal static DbCommand AddParameter(this DbCommand source, string name, object value)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));

            var parameter = source.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value ?? DBNull.Value;

            if (value is DateTime)
                parameter.DbType = DbType.DateTime2;

            source.Parameters.Add(parameter);

            //allow method-chaining
            return source;
        }
    }
}
