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
    using System.Globalization;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;
    using DurableTask.Core.Tracking;

    /// <summary>
    /// History Tracking Entity for orchestration jump start event
    /// </summary>
    public class AzureTableOrchestrationJumpStartEntity : AzureTableOrchestrationStateEntity
    {
        /// <summary>
        /// Gets or sets the datetime for the jumpstart event
        /// </summary>
        public DateTime JumpStartTime { get; set; }

        /// <summary>
        /// Creates a new AzureTableOrchestrationJumpStartEntity
        /// </summary>
        public AzureTableOrchestrationJumpStartEntity()
            : base()
        {
        }

        /// <summary>
        /// Creates a new AzureTableOrchestrationJumpStartEntity with the jumpstart state and datetime
        /// </summary>
        /// <param name="jumpStartEvent"></param>
        public AzureTableOrchestrationJumpStartEntity(OrchestrationJumpStartInstanceEntity jumpStartEvent)
            : base(jumpStartEvent.State)
        {
            this.JumpStartTime = jumpStartEvent.JumpStartTime;
        }

        /// <summary>
        /// Gets a OrchestrationJumpStartInstanceEntity
        /// </summary>
        public OrchestrationJumpStartInstanceEntity OrchestrationJumpStartInstanceEntity
        {
            get
            {
                return new OrchestrationJumpStartInstanceEntity()
                {
                    State = this.State,
                    JumpStartTime = this.JumpStartTime
                };
            }
        }

        internal override IEnumerable<ITableEntity> BuildDenormalizedEntities()
        {
            var entity1 = new AzureTableOrchestrationJumpStartEntity(this.OrchestrationJumpStartInstanceEntity);
            entity1.TaskTimeStamp = this.TaskTimeStamp;
            entity1.PartitionKey = GetPartitionKey(entity1.State.CreatedTime);
            entity1.RowKey = AzureTableConstants.InstanceStateExactRowPrefix +
                             AzureTableConstants.JoinDelimiter + State.OrchestrationInstance.InstanceId +
                             AzureTableConstants.JoinDelimiter + State.OrchestrationInstance.ExecutionId;
            return new AzureTableOrchestrationJumpStartEntity[1] { entity1 };
        }

        /// <summary>
        /// Write an entity to a dictionary of entity properties
        /// </summary>
        /// <param name="operationContext">The operation context</param>
        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            IDictionary<string, EntityProperty> retVals = base.WriteEntity(operationContext);
            retVals.Add("JumpStartTime", new EntityProperty(this.JumpStartTime));
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
            base.ReadEntity(properties, operationContext);
            this.JumpStartTime =
                GetValue("JumpStartTime", properties, property => property.DateTimeOffsetValue)
                    .GetValueOrDefault()
                    .DateTime;
        }

        /// <summary>
        /// Get a partition key based on a datetime
        /// </summary>
        /// <param name="dateTime">The datetime to use for the partition key</param>
        /// <returns>A string partition key</returns>
        public static string GetPartitionKey(DateTime dateTime)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0:D19}", dateTime.Ticks);
        }
    }
}