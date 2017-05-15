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
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.Serializing;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// History Tracking Entity for an orchestration's state
    /// </summary>
    public class AzureTableOrchestrationStateEntity : AzureTableCompositeTableEntity
    {
        readonly DataConverter dataConverter;

        /// <summary>
        /// Creates a new AzureTableOrchestrationStateEntity
        /// </summary>
        public AzureTableOrchestrationStateEntity()
        {
            this.dataConverter = new JsonDataConverter();
        }

        /// <summary>
        /// Creates a new AzureTableOrchestrationStateEntity with the supplied orchestration state
        /// </summary>
        /// <param name="state">The orchestration state</param>
        public AzureTableOrchestrationStateEntity(OrchestrationState state)
            :this()
        {
            State = state;
            TaskTimeStamp = state.CompletedTime;
        }

        /// <summary>
        /// Gets or sets the orchestraion state for the entity
        /// </summary>
        public OrchestrationState State { get; set; }

        internal override IEnumerable<ITableEntity> BuildDenormalizedEntities()
        {
            var entity1 = new AzureTableOrchestrationStateEntity(State);

            entity1.PartitionKey = AzureTableConstants.InstanceStatePrefix;
            entity1.RowKey = AzureTableConstants.InstanceStateExactRowPrefix +
                             AzureTableConstants.JoinDelimiter + State.OrchestrationInstance.InstanceId +
                             AzureTableConstants.JoinDelimiter + State.OrchestrationInstance.ExecutionId;

            return new AzureTableOrchestrationStateEntity[1] {entity1};

            // TODO : additional indexes for efficient querying in the future
        }

        /// <summary>
        /// Write an entity to a dictionary of entity properties
        /// </summary>
        /// <param name="operationContext">The operation context</param>
        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var retVals = new Dictionary<string, EntityProperty>();

            retVals.Add("InstanceId", new EntityProperty(State.OrchestrationInstance.InstanceId));
            retVals.Add("ExecutionId", new EntityProperty(State.OrchestrationInstance.ExecutionId));

            if (State.ParentInstance != null)
            {
                retVals.Add("ParentInstanceId",
                    new EntityProperty(State.ParentInstance.OrchestrationInstance.InstanceId));
                retVals.Add("ParentExecutionId",
                    new EntityProperty(State.ParentInstance.OrchestrationInstance.ExecutionId));
            }

            retVals.Add("TaskTimeStamp", new EntityProperty(TaskTimeStamp));
            retVals.Add("Name", new EntityProperty(State.Name));
            retVals.Add("Version", new EntityProperty(State.Version));
            retVals.Add("Status", new EntityProperty(State.Status));
            retVals.Add("Tags", new EntityProperty(State.Tags != null ? dataConverter.Serialize(State.Tags) : null));
            retVals.Add("OrchestrationStatus", new EntityProperty(State.OrchestrationStatus.ToString()));
            retVals.Add("CreatedTime", new EntityProperty(State.CreatedTime));
            retVals.Add("CompletedTime", new EntityProperty(State.CompletedTime));
            retVals.Add("LastUpdatedTime", new EntityProperty(State.LastUpdatedTime));
            retVals.Add("Size", new EntityProperty(State.Size));
            retVals.Add("CompressedSize", new EntityProperty(State.CompressedSize));
            retVals.Add("Input", new EntityProperty(State.Input.Truncate(ServiceBusConstants.MaxStringLengthForAzureTableColumn)));
            retVals.Add("Output", new EntityProperty(State.Output.Truncate(ServiceBusConstants.MaxStringLengthForAzureTableColumn)));

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
            State = new OrchestrationState
            {
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = GetValue("InstanceId", properties, property => property.StringValue),
                    ExecutionId = GetValue("ExecutionId", properties, property => property.StringValue)
                },
                ParentInstance = new ParentInstance
                {
                    Name = null,
                    TaskScheduleId = -1,
                    Version = null,
                    OrchestrationInstance = new OrchestrationInstance
                    {
                        InstanceId = GetValue("ParentInstanceId", properties, property => property.StringValue),
                        ExecutionId = GetValue("ParentExecutionId", properties, property => property.StringValue)
                    }
                },
                Name = GetValue("Name", properties, property => property.StringValue),
                Version = GetValue("Version", properties, property => property.StringValue),
                Status = GetValue("Status", properties, property => property.StringValue),
                Tags = GetTagsFromString(properties),
                CreatedTime =
                    GetValue("CreatedTime", properties, property => property.DateTimeOffsetValue)
                        .GetValueOrDefault()
                        .DateTime,
                CompletedTime =
                    GetValue("CompletedTime", properties, property => property.DateTimeOffsetValue)
                        .GetValueOrDefault()
                        .DateTime,
                LastUpdatedTime =
                    GetValue("LastUpdatedTime", properties, property => property.DateTimeOffsetValue)
                        .GetValueOrDefault()
                        .DateTime,
                Size = GetValue("Size", properties, property => property.Int64Value).GetValueOrDefault(),
                CompressedSize =
                    GetValue("CompressedSize", properties, property => property.Int64Value).GetValueOrDefault(),
                Input = GetValue("Input", properties, property => property.StringValue),
                Output = GetValue("Output", properties, property => property.StringValue)
            };

            TaskTimeStamp =
                GetValue("TaskTimeStamp", properties, property => property.DateTimeOffsetValue)
                    .GetValueOrDefault()
                    .DateTime;

            string orchestrationStatusStr = GetValue("OrchestrationStatus", properties, property => property.StringValue);
            if (!Enum.TryParse(orchestrationStatusStr, out State.OrchestrationStatus))
            {
                throw new InvalidOperationException("Invalid status string in state " + orchestrationStatusStr);
            }
        }

        private IDictionary<string, string> GetTagsFromString(IDictionary<string, EntityProperty> properties)
        {
            string strTags = GetValue("Tags", properties, property => property.StringValue);
            if (string.IsNullOrEmpty(strTags))
            {
                return null;
            }

            return dataConverter.Deserialize<IDictionary<string, string>>(strTags);
        }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        /// <returns>
        /// A string that represents the current object.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override string ToString()
        {
            return string.Format(
                "Instance Id: {0} Execution Id: {1} Name: {2} Version: {3} CreatedTime: {4} CompletedTime: {5} LastUpdated: {6} Status: {7} User Status: {8} Input: {9} Output: {10} Size: {11} CompressedSize: {12}",
                State.OrchestrationInstance.InstanceId, State.OrchestrationInstance.ExecutionId, State.Name,
                State.Version, State.CreatedTime, State.CompletedTime,
                State.LastUpdatedTime, State.OrchestrationStatus, State.Status, State.Input, State.Output, State.Size,
                State.CompressedSize);
        }
    }
}