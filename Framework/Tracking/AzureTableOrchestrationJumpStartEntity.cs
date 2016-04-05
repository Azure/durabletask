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
    using System.Globalization;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;

    public class AzureTableOrchestrationJumpStartEntity : AzureTableOrchestrationStateEntity
    {
        public DateTime JumpStartTime { get; set; }

        public AzureTableOrchestrationJumpStartEntity()
            : base()
        {
        }

        public AzureTableOrchestrationJumpStartEntity(OrchestrationJumpStartEvent jumpStartEvent)
            : base(jumpStartEvent.State)
        {
            this.JumpStartTime = jumpStartEvent.JumpStartTime;
        }

        public OrchestrationJumpStartEvent OrchestrationJumpStartEvent
        {
            get
            {
                return new OrchestrationJumpStartEvent()
                {
                    State = this.State,
                    JumpStartTime = this.JumpStartTime
                };
            }
        }

        internal override IEnumerable<ITableEntity> BuildDenormalizedEntities()
        {
            var entity1 = new AzureTableOrchestrationJumpStartEntity(this.OrchestrationJumpStartEvent);
            entity1.TaskTimeStamp = this.TaskTimeStamp;
            entity1.PartitionKey = GetPartitionKey(entity1.State.CreatedTime);
            entity1.RowKey = AzureTableConstants.InstanceStateExactRowPrefix +
                             AzureTableConstants.JoinDelimiter + State.OrchestrationInstance.InstanceId +
                             AzureTableConstants.JoinDelimiter + State.OrchestrationInstance.ExecutionId;
            return new AzureTableOrchestrationJumpStartEntity[1] { entity1 };
        }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            IDictionary<string, EntityProperty> retVals = base.WriteEntity(operationContext);
            retVals.Add("JumpStartTime", new EntityProperty(this.JumpStartTime));
            return retVals;
        }

        public override void ReadEntity(IDictionary<string, EntityProperty> properties,
            OperationContext operationContext)
        {
            base.ReadEntity(properties, operationContext);
            this.JumpStartTime =
                GetValue("JumpStartTime", properties, property => property.DateTimeOffsetValue)
                    .GetValueOrDefault()
                    .DateTime;
        }

        public static string GetPartitionKey(DateTime dateTime)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0:D19}", dateTime.Ticks);
        }
    }
}