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
    using Azure.Data.Tables;
    using DurableTask.Core.Tracking;

    /// <summary>
    /// History Tracking Entity for orchestration jump start event
    /// </summary>
    public class AzureTableOrchestrationJumpStartEntity : AzureTableOrchestrationStateEntity
    {
        /// <summary>
        /// Gets or sets the date and time for the jump start event
        /// </summary>
        public DateTime JumpStartTime { get; set; }

        /// <summary>
        /// Creates a new AzureTableOrchestrationJumpStartEntity
        /// </summary>
        public AzureTableOrchestrationJumpStartEntity()
        {
        }

        /// <summary>
        /// Creates a new AzureTableOrchestrationJumpStartEntity with the jump start state and datetime
        /// </summary>
        /// <param name="jumpStartEvent"></param>
        public AzureTableOrchestrationJumpStartEntity(OrchestrationJumpStartInstanceEntity jumpStartEvent)
            : base(jumpStartEvent.State)
        {
            JumpStartTime = jumpStartEvent.JumpStartTime;
        }

        /// <summary>
        /// Gets a OrchestrationJumpStartInstanceEntity
        /// </summary>
        public OrchestrationJumpStartInstanceEntity OrchestrationJumpStartInstanceEntity => new OrchestrationJumpStartInstanceEntity
        {
            State = State,
            JumpStartTime = JumpStartTime
        };

        internal override IEnumerable<ITableEntity> BuildDenormalizedEntities()
        {
            var entity1 = new AzureTableOrchestrationJumpStartEntity(OrchestrationJumpStartInstanceEntity);
            entity1.TaskTimeStamp = TaskTimeStamp;
            entity1.PartitionKey = GetPartitionKey(entity1.State.CreatedTime);
            entity1.RowKey = AzureTableConstants.InstanceStateExactRowPrefix +
                             AzureTableConstants.JoinDelimiter + State.OrchestrationInstance.InstanceId +
                             AzureTableConstants.JoinDelimiter + State.OrchestrationInstance.ExecutionId;
            entity1.ETag = ETag;
            return new [] { entity1 };
        }

        /// <summary>
        /// Write an entity to a dictionary of entity properties
        /// </summary>
        /// <param name="operationContext">The operation context</param>
        public override IDictionary<string, object> WriteEntity()
        {
            //TODO: operationContext
            IDictionary<string, object> returnValues = base.WriteEntity();
            returnValues.Add("JumpStartTime", JumpStartTime);
            return returnValues;
        }

        /// <summary>
        /// Read an entity properties based on the supplied dictionary or entity properties
        /// </summary>
        /// <param name="properties">Dictionary of properties to read for the entity</param>
        /// <param name="operationContext">The operation context</param>
        public override void ReadEntity(IDictionary<string, object> properties)
        {
            //TODO: operationContext
            base.ReadEntity(properties);
            JumpStartTime =
                GetValue("JumpStartTime", properties, property => property as DateTimeOffset?)
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
