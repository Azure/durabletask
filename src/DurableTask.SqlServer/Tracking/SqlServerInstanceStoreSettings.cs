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


namespace DurableTask.SqlServer.Tracking
{
    using System;
    using System.Data.Common;
    using System.Threading.Tasks;

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
