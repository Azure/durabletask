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
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.Serializing;

    /// <summary>
    /// History Tracking Entity for an orchestration's state
    /// </summary>
    internal class AzureTableOrchestrationStateEntity : AzureTableCompositeTableEntity
    {
        [IgnoreDataMember] // This property needs to be ignored to safely be skipped by the Azure Storage SDK
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
        [IgnoreDataMember] // See AzureStorageHelpers Region
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

        #region AzureStorageHelpers
        // Public Accessors with the Sate prefix are equivalent to those in the already existing State Property.
        // These are used only to safely interact with the Azure Table Storage SDK which reads and writes using reflection.
        // This is an artifact of updating from an SDK that did not use reflection.

        [DataMember(Name = "InstanceId")]
        public string StateInstanceId
        {
            get => State.OrchestrationInstance.InstanceId;
            set
            {
                CreateStateIfNotExists();
                State.OrchestrationInstance.InstanceId = value;
            }
        }

        [DataMember(Name = "ExecutionId")]
        public string StateExecutionId
        {
            get => State.OrchestrationInstance.ExecutionId;
            set
            {
                CreateStateIfNotExists();
                State.OrchestrationInstance.ExecutionId = value;
            }
        }

        [DataMember(Name = "ParentInstanceId")]
        public string StateParentInstanceId
        {
            get => State.ParentInstance?.OrchestrationInstance.InstanceId;
            set
            {
                CreateStateIfNotExists();
                State.ParentInstance.OrchestrationInstance.InstanceId = value;
            }
        }

        [DataMember(Name = "ParentExecutionId")]
        public string StateParentExecutionId
        {
            get => State.ParentInstance?.OrchestrationInstance.ExecutionId;
            set
            {
                CreateStateIfNotExists();
                State.ParentInstance.OrchestrationInstance.ExecutionId = value;
            }
        }

        [DataMember(Name = "Name")]
        public string StateName
        {
            get => State.Name;
            set
            {
                CreateStateIfNotExists();
                State.Name = value;
            }
        }

        [DataMember(Name = "Version")]
        public string StateVersion
        {
            get => State.Version;
            set
            {
                CreateStateIfNotExists();
                State.Version = value;
            }
        }

        [DataMember(Name = "Status")]
        public string StateStatus
        {
            get => State.Status;
            set
            {
                CreateStateIfNotExists();
                State.Status = value;
            }
        }

        [DataMember(Name = "Tags")]
        public string StateTags
        {
            get => State.Tags != null ? this.dataConverter.Serialize(State.Tags) : null;
            set
            {
                CreateStateIfNotExists();
                if (string.IsNullOrWhiteSpace(value))
                {
                    State.Tags = null;
                }

                State.Tags = this.dataConverter.Deserialize<IDictionary<string, string>>(value);
            }
        }

        [DataMember(Name = "OrchestrationStatus")]
        public string StateOrchestrationStatus
        {
            get => State.OrchestrationStatus.ToString();
            set
            {
                CreateStateIfNotExists();
                if (!Enum.TryParse(value, out State.OrchestrationStatus))
                {
                    throw new InvalidOperationException("Invalid status string in state " + value);
                }
            }
        }

        [DataMember(Name = "CreatedTime")]
        public DateTime StateCreatedTime
        {
            get => State.CreatedTime;
            set
            {
                CreateStateIfNotExists();
                State.CreatedTime = value;
            }
        }

        [DataMember(Name = "CompletedTime")]
        public DateTime StateCompletedTime
        {
            get => State.CompletedTime;
            set
            {
                CreateStateIfNotExists();
                State.CompletedTime = value;
            }
        }

        [DataMember(Name = "LastUpdatedTime")]
        public DateTime StateLastUpdatedTime
        {
            get => State.LastUpdatedTime;
            set
            {
                CreateStateIfNotExists();
                State.LastUpdatedTime = value;
            }
        }

        [DataMember(Name = "Size")]
        public long StateSize
        {
            get => State.Size;
            set
            {
                CreateStateIfNotExists();
                State.Size = value;
            }
        }

        [DataMember(Name = "CompressedSize")]
        public long StateCompressedSize
        {
            get => State.CompressedSize;
            set
            {
                CreateStateIfNotExists();
                State.CompressedSize = value;
            }
        }

        [DataMember(Name = "Input")]
        public string StateInput
        {
            get => State.Input.Truncate(ServiceBusConstants.MaxStringLengthForAzureTableColumn);
            set
            {
                CreateStateIfNotExists();
                State.Input = value;
            }

        }

        [DataMember(Name = "Output")]
        public string StateOutput
        {
            get => State.Output.Truncate(ServiceBusConstants.MaxStringLengthForAzureTableColumn);
            set
            {
                CreateStateIfNotExists();
                State.Output = value;
            }
        }

        [DataMember(Name = "ScheduledStartTime")]
        public DateTime? StateScheduledStartTime
        {
            get => State.ScheduledStartTime;
            set
            {
                CreateStateIfNotExists();
                State.ScheduledStartTime = value;
            }
        }

        private void CreateStateIfNotExists()
        {
            if (this.State == null)
            {
                this.State = new OrchestrationState
                {
                    OrchestrationInstance = new OrchestrationInstance(),
                    ParentInstance = new ParentInstance
                    {
                        Name = null,
                        TaskScheduleId = -1,
                        Version = null,
                        OrchestrationInstance = new OrchestrationInstance(),
                    }
                };
            }
            if (this.State.OrchestrationInstance == null)
            {
                this.State.OrchestrationInstance = new OrchestrationInstance();
            }
            if (this.State.ParentInstance == null)
            {
                this.State.ParentInstance = new ParentInstance
                {
                    Name = null,
                    TaskScheduleId = -1,
                    Version = null,
                    OrchestrationInstance = new OrchestrationInstance(),
                };
            }
        }
        #endregion
    }
}