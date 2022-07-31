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
    using Azure.Data.Tables;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.Serializing;

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
            this.dataConverter = JsonDataConverter.Default;
        }

        /// <summary>
        /// Creates a new AzureTableOrchestrationStateEntity with the supplied orchestration state
        /// </summary>
        /// <param name="state">The orchestration state</param>
        public AzureTableOrchestrationStateEntity(OrchestrationState state)
            : this()
        {
            State = state;
            TaskTimeStamp = state.CompletedTime;
        }

        /// <summary>
        /// Gets or sets the orchestration state for the entity
        /// </summary>
        public OrchestrationState State { get; set; }

        internal override IEnumerable<ITableEntity> BuildDenormalizedEntities()
        {
            var entity1 = new AzureTableOrchestrationStateEntity(State);

            entity1.PartitionKey = AzureTableConstants.InstanceStatePrefix;
            entity1.RowKey = AzureTableConstants.InstanceStateExactRowPrefix +
                             AzureTableConstants.JoinDelimiter + State.OrchestrationInstance.InstanceId +
                             AzureTableConstants.JoinDelimiter + State.OrchestrationInstance.ExecutionId;

            return new [] { entity1 };
            // TODO : additional indexes for efficient querying in the future
        }

        /// <summary>
        /// Write an entity to a dictionary of entity properties
        /// </summary>
        public override IDictionary<string, object> WriteEntity()
        {
            var returnValues = new Dictionary<string, object>();

            returnValues.Add("InstanceId", State.OrchestrationInstance.InstanceId);
            returnValues.Add("ExecutionId", State.OrchestrationInstance.ExecutionId);

            if (State.ParentInstance != null)
            {
                returnValues.Add("ParentInstanceId", State.ParentInstance.OrchestrationInstance.InstanceId);
                returnValues.Add("ParentExecutionId", State.ParentInstance.OrchestrationInstance.ExecutionId);
            }

            returnValues.Add("TaskTimeStamp", TaskTimeStamp);
            returnValues.Add("Name", State.Name);
            returnValues.Add("Version", State.Version);
            returnValues.Add("Status", State.Status);
            returnValues.Add("Tags", State.Tags != null ? this.dataConverter.Serialize(State.Tags) : null);
            returnValues.Add("OrchestrationStatus", State.OrchestrationStatus.ToString());
            returnValues.Add("CreatedTime", State.CreatedTime);
            returnValues.Add("CompletedTime", State.CompletedTime);
            returnValues.Add("LastUpdatedTime", State.LastUpdatedTime);
            returnValues.Add("Size", State.Size);
            returnValues.Add("CompressedSize", State.CompressedSize);
            returnValues.Add("Input", State.Input.Truncate(ServiceBusConstants.MaxStringLengthForAzureTableColumn));
            returnValues.Add("Output", State.Output.Truncate(ServiceBusConstants.MaxStringLengthForAzureTableColumn));
            returnValues.Add("ScheduledStartTime", State.ScheduledStartTime);

            return returnValues;
        }

        /// <summary>
        /// Read an entity properties based on the supplied dictionary or entity properties
        /// </summary>
        /// <param name="properties">Dictionary of properties to read for the entity</param>
        public override void ReadEntity(IDictionary<string, object> properties)
        {
            State = new OrchestrationState
            {
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = GetValue<string>("InstanceId", properties),
                    ExecutionId = GetValue<string>("ExecutionId", properties),
                },
                ParentInstance = new ParentInstance
                {
                    Name = null,
                    TaskScheduleId = -1,
                    Version = null,
                    OrchestrationInstance = new OrchestrationInstance
                    {
                        InstanceId = GetValue<string>("ParentInstanceId", properties),
                        ExecutionId = GetValue<string>("ParentExecutionId", properties),
                    }
                },
                Name = GetValue<string>("Name", properties),
                Version = GetValue<string>("Version", properties),
                Status = GetValue<string>("Status", properties),
                Tags = GetTagsFromString(properties),
                CreatedTime =
                    GetValue<DateTimeOffset?>("CreatedTime", properties)
                        .GetValueOrDefault()
                        .DateTime,
                CompletedTime =
                    GetValue<DateTimeOffset?>("CompletedTime", properties)
                        .GetValueOrDefault()
                        .DateTime,
                LastUpdatedTime =
                    GetValue<DateTimeOffset?>("LastUpdatedTime", properties)
                        .GetValueOrDefault()
                        .DateTime,
                Size = GetValue<long?>("Size", properties).GetValueOrDefault(),
                CompressedSize =
                    GetValue<long?>("CompressedSize", properties).GetValueOrDefault(),
                Input = GetValue<string>("Input", properties),
                Output = GetValue<string>("Output", properties),
                ScheduledStartTime = GetValue<DateTimeOffset?>("ScheduledStartTime", properties)
                    .GetValueOrDefault()
                    .DateTime,
            };

            TaskTimeStamp = GetValue<DateTimeOffset?>("TaskTimeStamp", properties)
                .GetValueOrDefault()
                .DateTime;

            string orchestrationStatusStr = GetValue<string>("OrchestrationStatus", properties);
            if (!Enum.TryParse(orchestrationStatusStr, out State.OrchestrationStatus))
            {
                throw new InvalidOperationException("Invalid status string in state " + orchestrationStatusStr);
            }
        }

        private IDictionary<string, string> GetTagsFromString(IDictionary<string, object> properties)
        {
            string strTags = GetValue<string>("Tags", properties);
            if (string.IsNullOrWhiteSpace(strTags))
            {
                return null;
            }

            return this.dataConverter.Deserialize<IDictionary<string, string>>(strTags);
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
            // ReSharper disable once UseStringInterpolation
            return string.Format(
                "Instance Id: {0} Execution Id: {1} Name: {2} Version: {3} CreatedTime: {4} CompletedTime: {5} LastUpdated: {6} Status: {7} User Status: {8} Input: {9} Output: {10} Size: {11} CompressedSize: {12}",
                State.OrchestrationInstance.InstanceId, State.OrchestrationInstance.ExecutionId, State.Name,
                State.Version, State.CreatedTime, State.CompletedTime,
                State.LastUpdatedTime, State.OrchestrationStatus, State.Status, State.Input, State.Output, State.Size,
                State.CompressedSize);
        }
    }
}