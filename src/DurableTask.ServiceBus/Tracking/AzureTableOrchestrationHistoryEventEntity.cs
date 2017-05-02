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
    using System;
    using System.Collections.Generic;
    using DurableTask.Core.History;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;

    /// <summary>
    /// History Tracking entity for orchestration history events
    /// </summary>
    public class AzureTableOrchestrationHistoryEventEntity : AzureTableCompositeTableEntity
    {
        /// <summary>
        /// Creates a new AzureTableOrchestrationHistoryEventEntity 
        /// </summary>
        public AzureTableOrchestrationHistoryEventEntity()
        {
        }

        /// <summary>
        /// Creates a new AzureTableOrchestrationHistoryEventEntity with supplied parameters
        /// </summary>
        /// <param name="instanceId">The orchestation instance id</param>
        /// <param name="executionId">The orchestration execution id</param>
        /// <param name="sequenceNumber">Sequence number for ordering events</param>
        /// <param name="taskTimeStamp">Timestamp of history event</param>
        /// <param name="historyEvent">The history event details</param>
        public AzureTableOrchestrationHistoryEventEntity(
            string instanceId, 
            string executionId, 
            long sequenceNumber,
            DateTime taskTimeStamp, 
            HistoryEvent historyEvent)
        {
            InstanceId = instanceId;
            ExecutionId = executionId;
            SequenceNumber = (int) sequenceNumber;
            TaskTimeStamp = taskTimeStamp;
            HistoryEvent = historyEvent;

            PartitionKey = AzureTableConstants.InstanceHistoryEventPrefix +
                           AzureTableConstants.JoinDelimiter + InstanceId;
            RowKey = ExecutionId + AzureTableConstants.JoinDelimiter + SequenceNumber;
        }

        /// <summary>
        /// Gets or set the instance id of the orchestration
        /// </summary>
        public string InstanceId { get; set; }

        /// <summary>
        /// Gets or set the execution id of the orchestration
        /// </summary>
        public string ExecutionId { get; set; }

        /// <summary>
        /// Gets or set the sequence number for ordering events
        /// </summary>
        public int SequenceNumber { get; set; }

        /// <summary>
        /// Gets or set the history event detail for the tracking entity
        /// </summary>
        public HistoryEvent HistoryEvent { get; set; }

        internal override IEnumerable<ITableEntity> BuildDenormalizedEntities()
        {
            var entity = new AzureTableOrchestrationHistoryEventEntity(InstanceId,
                ExecutionId,
                SequenceNumber,
                TaskTimeStamp, HistoryEvent);
            entity.PartitionKey = AzureTableConstants.InstanceHistoryEventPrefix +
                                  AzureTableConstants.JoinDelimiter + InstanceId;
            entity.RowKey = AzureTableConstants.InstanceHistoryEventRowPrefix +
                            AzureTableConstants.JoinDelimiter +
                            ExecutionId + AzureTableConstants.JoinDelimiter + SequenceNumber;

            return new AzureTableOrchestrationHistoryEventEntity[1] {entity};
        }

        /// <summary>
        /// Write an entity to a dictionary of entity properties
        /// </summary>
        /// <param name="operationContext">The operation context</param>
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
                serializedHistoryEvent.Length > ServiceBusConstants.MaxStringLengthForAzureTableColumn)
            {
                serializedHistoryEvent = JsonConvert.SerializeObject(new GenericEvent(HistoryEvent.EventId,
                    serializedHistoryEvent.Substring(0, ServiceBusConstants.MaxStringLengthForAzureTableColumn) + " ....(truncated)..]"),
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

        /// <summary>
        /// Read an entity properties based on the supplied dictionary or entity properties
        /// </summary>
        /// <param name="properties">Dictionary of properties to read for the entity</param>
        /// <param name="operationContext">The operation context</param>
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

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString()
        {
            return string.Format("Instance Id: {0} Execution Id: {1} Seq: {2} Time: {3} HistoryEvent: {4}",
                InstanceId, ExecutionId, SequenceNumber, TaskTimeStamp, HistoryEvent.EventType);
        }
    }
}