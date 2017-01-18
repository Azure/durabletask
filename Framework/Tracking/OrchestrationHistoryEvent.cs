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
//  ----------------------------------------------------------------------------------using System;

using DurableTask.History;
using System;

namespace DurableTask.Tracking
{
    /// <summary>
    /// Defines the class which describes the history event of an orchestration.
    /// </summary>
    public class OrchestrationHistoryEvent
    {
        /// <summary>
        /// Orchestration Instance identifier.
        /// </summary>
        public string InstanceId { get; set; }

        /// <summary>
        /// Execution Identifier.
        /// </summary>
        public string ExecutionId { get; set; }

        /// <summary>
        /// Sequence Number
        /// </summary>
        public int SequenceNumber { get; set; }

        /// <summary>
        /// History Event
        /// </summary>
        public HistoryEvent HistoryEvent { get; set; }

        /// <summary>
        /// Timestamp of related task.
        /// </summary>
        public DateTime TaskTimeStamp { get; set; }    

        /// <summary>
        /// Timestamp of the event.
        /// </summary>
        public DateTimeOffset Timestamp { get; set; }


        /// <summary>
        /// Creates the instance of event from message properties.
        /// </summary>
        /// <param name="instanceId"></param>
        /// <param name="executionId"></param>
        /// <param name="sequenceNumber"></param>
        /// <param name="taskTimeStamp"></param>
        /// <param name="historyEvent"></param>
        public OrchestrationHistoryEvent(string instanceId, 
            string executionId, int sequenceNumber,
         DateTime taskTimeStamp, HistoryEvent historyEvent)
        {
            InstanceId = instanceId;
            ExecutionId = executionId;
            SequenceNumber = sequenceNumber;
            TaskTimeStamp = taskTimeStamp;
            HistoryEvent = historyEvent;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format("Instance Id: {0} Execution Id: {1} Seq: {2} Time: {3} HistoryEvent: {4}",
                InstanceId, ExecutionId, SequenceNumber, TaskTimeStamp, HistoryEvent.EventType);
        }
    }

   
}
