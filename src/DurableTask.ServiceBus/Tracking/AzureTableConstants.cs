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

namespace DurableTask.ServiceBus.Tracking
{
    internal class AzureTableConstants
    {
        // Table name: InstanceHistory00<hub_name>
        public const string InstanceHistoryTableNamePrefix = "InstanceHistory";

        // Table name: JumpStart<hub_name>
        public const string JumpStartTableNamePrefix = "JumpStart";

        // HistoryEvent related prefixes, only one row per history event
        public const string InstanceHistoryEventPrefix = "IH";
        public const string InstanceHistoryEventRowPrefix = "EID" + JoinDelimiter + "SEQ";

        // Log related prefixes, two rows per log:
        //          index by EID
        //          index by type
        public const string InstanceLogPrefix = "IL";
        public const string InstanceLogExecutionRowPrefix = "EID" + JoinDelimiter + "TYPE";
        public const string InstanceLogTypeRowPrefix = "TYPE" + JoinDelimiter + "EID";

        // State related prefixes, four rows per state:
        //          index by Id/EID
        //          index by name
        //          index by state
        //          index by completion date
        public const string InstanceStatePrefix = "IS";
        public const string InstanceStateExactRowPrefix = "ID" + JoinDelimiter + "EID";
        public const string InstanceStateNameRowPrefix = "ONAME" + JoinDelimiter + InstanceStateExactRowPrefix;
        public const string InstanceStateStateRowPrefix = "STATE" + JoinDelimiter + InstanceStateExactRowPrefix;

        public const string InstanceStateCompleteDateRowPrefix =
            "COMPDATE" + JoinDelimiter + InstanceStateExactRowPrefix;

        public const string JoinDelimiter = "_";
        public const string JoinDelimiterPlusOne = "`";


        // table query filters

        // general primary filters
        public const string TableExactQueryFormat = "(PartitionKey eq '{0}') and (RowKey eq '{1}')";

        public const string TableRangeQueryFormat =
            "((PartitionKey eq '{0}') and (RowKey ge '{1}') and (RowKey lt '{2}'))";

        // primary filters 
        public const string PrimaryFilterTemplate = "(PartitionKey eq 'IS')";
        public const string PrimaryTimeRangeTemplate = "((PartitionKey ge '{0}') and (PartitionKey lt '{1}'))";

        // rowkey format: ID_EID_<instanceId>_<executionId>
        public const string PrimaryInstanceQueryExactTemplate =
            "(RowKey eq '" + InstanceStateExactRowPrefix + JoinDelimiter + "{0}" + JoinDelimiter + "{1}')";
        public const string PrimaryInstanceQueryRangeTemplate =
            "(RowKey ge '" + InstanceStateExactRowPrefix + JoinDelimiter + "{0}') and (RowKey lt '" + InstanceStateExactRowPrefix + JoinDelimiter + "{1}')";

        // secondary filters
        public const string InstanceQuerySecondaryFilterTemplate = "(InstanceId eq '{0}')";

        public const string InstanceQuerySecondaryFilterRangeTemplate =
            "((InstanceId ge '{0}') and (InstanceId lt '{1}'))";

        public const string InstanceQuerySecondaryFilterExactTemplate =
            "((InstanceId eq '{0}') and (ExecutionId eq '{1}'))";

        public const string NameVersionQuerySecondaryFilterTemplate = "(Name eq '{0}')";
        public const string NameVersionQuerySecondaryFilterExactTemplate = "((Name eq '{0}') and (Version eq '{1}'))";

        public const string StatusQuerySecondaryFilterTemplate = "(OrchestrationStatus eq '{0}')";

        public const string CreatedTimeRangeQuerySecondaryFilterTemplate =
            "((CreatedTime ge datetime'{0}') and (CreatedTime lt datetime'{1}')) ";

        public const string CompletedTimeRangeQuerySecondaryFilterTemplate =
            "((CompletedTime ge datetime'{0}') and (CompletedTime lt datetime'{1}')) ";

        public const string LastUpdatedTimeRangeQuerySecondaryFilterTemplate =
            "((LastUpdatedTime ge datetime'{0}') and (LastUpdatedTime lt datetime'{1}')) ";
    }
}