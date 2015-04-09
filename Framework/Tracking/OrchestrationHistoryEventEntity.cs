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

namespace DurableTask.Tracking
{
    using System;
    using System.Collections.Generic;
    using History;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;

    public class OrchestrationHistoryEventEntity : CompositeTableEntity
    {
        public OrchestrationHistoryEventEntity()
        {
        }

        public OrchestrationHistoryEventEntity(string instanceId, string executionId, int sequenceNumber,
            DateTime taskTimeStamp, HistoryEvent historyEvent)
        {
            InstanceId = instanceId;
            ExecutionId = executionId;
            SequenceNumber = sequenceNumber;
            TaskTimeStamp = taskTimeStamp;
            HistoryEvent = historyEvent;

            PartitionKey = TableConstants.InstanceHistoryEventPrefix +
                           TableConstants.JoinDelimiter + InstanceId;
            RowKey = ExecutionId + TableConstants.JoinDelimiter + SequenceNumber;
        }

        public string InstanceId { get; set; }
        public string ExecutionId { get; set; }
        public int SequenceNumber { get; set; }
        public HistoryEvent HistoryEvent { get; set; }

        internal override IEnumerable<ITableEntity> BuildDenormalizedEntities()
        {
            var entity = new OrchestrationHistoryEventEntity(InstanceId,
                ExecutionId,
                SequenceNumber,
                TaskTimeStamp, HistoryEvent);
            entity.PartitionKey = TableConstants.InstanceHistoryEventPrefix +
                                  TableConstants.JoinDelimiter + InstanceId;
            entity.RowKey = TableConstants.InstanceHistoryEventRowPrefix +
                            TableConstants.JoinDelimiter +
                            ExecutionId + TableConstants.JoinDelimiter + SequenceNumber;

            return new OrchestrationHistoryEventEntity[1] {entity};
        }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            string serializedHistoryEvent = JsonConvert.SerializeObject(HistoryEvent,
                new JsonSerializerSettings
                {
                    Formatting = Formatting.Indented,
                    TypeNameHandling = TypeNameHandling.Objects
                });

            // replace with a generic event with the truncated history so at least we have some record
            // note that this makes the history stored in the instance store unreplayable. so any replay logic
            // that we build will have to especially check for this event and flag the orchestration as unplayable if it sees this event
            if (!string.IsNullOrEmpty(serializedHistoryEvent) &&
                serializedHistoryEvent.Length > FrameworkConstants.MaxStringLengthForAzureTableColumn)
            {
                serializedHistoryEvent = JsonConvert.SerializeObject(new GenericEvent(HistoryEvent.EventId,
                    serializedHistoryEvent.Substring(0, FrameworkConstants.MaxStringLengthForAzureTableColumn) + " ....(truncated)..]"),
                    new JsonSerializerSettings
                    {
                        Formatting = Formatting.Indented,
                        TypeNameHandling = TypeNameHandling.Objects
                    });
            }

            var retVals = new Dictionary<string, EntityProperty>();
            retVals.Add("InstanceId", new EntityProperty(InstanceId));
            retVals.Add("ExecutionId", new EntityProperty(ExecutionId));
            retVals.Add("TaskTimeStamp", new EntityProperty(TaskTimeStamp));
            retVals.Add("SequenceNumber", new EntityProperty(SequenceNumber));
            retVals.Add("HistoryEvent", new EntityProperty(serializedHistoryEvent));

            return retVals;
        }

        public override void ReadEntity(IDictionary<string, EntityProperty> properties,
            OperationContext operationContext)
        {
            InstanceId = GetValue("InstanceId", properties, property => property.StringValue);
            ExecutionId = GetValue("ExecutionId", properties, property => property.StringValue);
            SequenceNumber = GetValue("SequenceNumber", properties, property => property.Int32Value).GetValueOrDefault();
            TaskTimeStamp =
                GetValue("TaskTimeStamp", properties, property => property.DateTimeOffsetValue)
                    .GetValueOrDefault()
                    .DateTime;

            string serializedHistoryEvent = GetValue("HistoryEvent", properties, property => property.StringValue);
            HistoryEvent = JsonConvert.DeserializeObject<HistoryEvent>(serializedHistoryEvent,
                new JsonSerializerSettings {TypeNameHandling = TypeNameHandling.Objects});
        }

        public override string ToString()
        {
            return string.Format("Instance Id: {0} Execution Id: {1} Seq: {2} Time: {3} HistoryEvent: {4}",
                InstanceId, ExecutionId, SequenceNumber, TaskTimeStamp, HistoryEvent.EventType);
        }
    }
}