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
    using System.Runtime.Serialization;
    using Azure.Data.Tables;
    using DurableTask.Core.History;
    using DurableTask.Core.Serializing;
    using Newtonsoft.Json;

    /// <summary>
    /// History Tracking entity for orchestration history events
    /// </summary>
    internal class AzureTableOrchestrationHistoryEventEntity : AzureTableCompositeTableEntity
    {
        private static readonly JsonSerializerSettings WriteJsonSettings = new JsonSerializerSettings
        {
            Formatting = Formatting.Indented,
            TypeNameHandling = TypeNameHandling.Objects
        };

        private static readonly JsonSerializerSettings ReadJsonSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.Objects,
            SerializationBinder = new PackageUpgradeSerializationBinder()
        };

        /// <summary>
        /// Creates a new AzureTableOrchestrationHistoryEventEntity 
        /// </summary>
        public AzureTableOrchestrationHistoryEventEntity()
        {
        }

        /// <summary>
        /// Creates a new AzureTableOrchestrationHistoryEventEntity with supplied parameters
        /// </summary>
        /// <param name="instanceId">The orchestration instance id</param>
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
            SequenceNumber = (int)sequenceNumber;
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
        [IgnoreDataMember] // see HistoryEventJson
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

            return new[] { entity };
        }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        public override string ToString()
        {
            return $"Instance Id: {InstanceId} Execution Id: {ExecutionId} Seq: {SequenceNumber.ToString()} Time: {TaskTimeStamp} HistoryEvent: {HistoryEvent.EventType.ToString()}";
        }

        #region AzureStorageHelpers
        // This public accessor is only used to safely interact with the Azure Table Storage SDK, which reads and writes using reflection.
        // This is an artifact of updating from and SDK that did not use reflection.
        [DataMember(Name = "HistoryEvent")]
        public string HistoryEventJson
        {
            get
            {
                var serializedHistoryEvent = JsonConvert.SerializeObject(HistoryEvent, WriteJsonSettings);
                if (!string.IsNullOrWhiteSpace(serializedHistoryEvent) &&
                serializedHistoryEvent.Length > ServiceBusConstants.MaxStringLengthForAzureTableColumn)
                {
                    serializedHistoryEvent = JsonConvert.SerializeObject(new GenericEvent(HistoryEvent.EventId,
                        serializedHistoryEvent.Substring(0, ServiceBusConstants.MaxStringLengthForAzureTableColumn) + " ....(truncated)..]"),
                        WriteJsonSettings);
                }
                return serializedHistoryEvent;
            }
            set => HistoryEvent = JsonConvert.DeserializeObject<HistoryEvent>(value, ReadJsonSettings);
        }
        #endregion
    }
}