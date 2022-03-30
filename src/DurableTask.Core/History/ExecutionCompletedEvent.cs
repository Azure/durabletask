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
#nullable enable
namespace DurableTask.Core.History
{
    using System.Runtime.Serialization;

    /// <summary>
    /// A history event for execution completed
    /// </summary>
    [DataContract]
    public class ExecutionCompletedEvent : HistoryEvent
    {
        /// <summary>
        /// Creates a new ExecutionCompletedEvent with the supplied parameters
        /// </summary>
        /// <param name="eventId">The event integer id</param>
        /// <param name="result">The string serialized completion result</param>
        /// <param name="orchestrationStatus">The orchestration status</param>
        /// <param name="failureDetails">Structured details of the orchestration failure</param>
        public ExecutionCompletedEvent(int eventId, string? result, OrchestrationStatus orchestrationStatus, FailureDetails? failureDetails = null)
            : base(eventId)
        {
            Result = result;
            OrchestrationStatus = orchestrationStatus;
            FailureDetails = failureDetails;
        }

        /// <summary>
        /// Gets the event type
        /// </summary>
        public override EventType EventType => EventType.ExecutionCompleted;

        /// <summary>
        /// Gets the history events orchestration status
        /// </summary>
        [DataMember]
        public OrchestrationStatus OrchestrationStatus { get; private set; }

        /// <summary>
        /// Gets the serialized completion result
        /// </summary>
        [DataMember]
        public string? Result { get; private set; }

        /// <summary>
        /// Gets the structured details of the task failure.
        /// </summary>
        [DataMember]
        public FailureDetails? FailureDetails { get; private set; }
    }
}