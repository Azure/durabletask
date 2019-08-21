using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace DurableTask.SqlServer.Tracking
{
    /// <summary>
    /// Configuration values for the Instnace Store
    /// </summary>
    public class SqlServerInstanceStoreSettings
    {
        internal const string OrchestrationTable = "_OrchestrationState";
        internal const string WorkitemTable = "_WorkItem";

        /// <summary>
        /// Gets or sets the hub name for the databse instance store.
        /// </summary>
        public string HubName { get; set; }

        /// <summary>
        /// Gets or sets the schema name to which the tables will be added.
        /// </summary>
        public string SchemaName { get; set; } = "dbo";

        /// <summary>
        /// The schema and name of the Orchestration State table.
        /// </summary>
        public string OrchestrationStateTableName => $"[{SchemaName}].[{HubName}{OrchestrationTable}]";

        /// <summary>
        /// The schema nad name of the Work Item table.
        /// </summary>
        public string WorkItemTableName => $"[{SchemaName}].[{HubName}{WorkitemTable}]";

        /// <summary>
        /// The delegate used to retrieve a <see cref="DbConnection"/> instance.
        /// </summary>
        public Func<Task<DbConnection>> GetDatabaseConnection { get; set; }
    }
}
