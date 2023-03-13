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

namespace DurableTask.Core.Exceptions
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Represents errors experienced while executing an operation on an entity.
    /// </summary>
    [Serializable]
    public class OperationFailedException : OrchestrationException
    {
        /// <summary>
        /// Initializes an new instance of the OperationFailedException class
        /// </summary>
        public OperationFailedException()
        {
        }

        /// <summary>
        /// Initializes an new instance of the OperationFailedException class with a specified error message
        /// </summary>
        /// <param name="reason">The message that describes the error.</param>
        public OperationFailedException(string reason)
            : base(reason)
        {
        }

        /// <summary>
        /// Initializes an new instance of the OperationFailedException class with a specified error message
        ///    and a reference to the inner exception that is the cause of this exception.
        /// </summary>
        /// <param name="reason">The message that describes the error.</param>
        /// <param name="innerException">The exception that is the cause of the current exception, or a null reference if no inner exception is specified.</param>
        public OperationFailedException(string reason, Exception innerException)
            : base(reason, innerException)
        {
        }

        /// <summary>
        /// Initializes an new instance of the OperationFailedException class with a specified event id, schedule id, name, version and error message
        ///    and a reference to the inner exception that is the cause of this exception.
        /// </summary>
        /// <param name="eventId">EventId of the error.</param>
        /// <param name="scheduleId">ScheduleId of the error.</param>
        /// <param name="instanceId">The instance id of the entity.</param>
        /// <param name="operationId">The operation id of the operation.</param>
        /// <param name="reason">The message that describes the error.</param>
        /// <param name="cause">The exception that is the cause of the current exception, or a null reference if no cause is specified.</param>
        public OperationFailedException(int eventId, int scheduleId, string instanceId, string operationId, string reason,
            Exception cause)
            : base(eventId, reason, cause)
        {
            ScheduleId = scheduleId;
            InstanceId = instanceId;
            OperationId = operationId;
        }

        /// <summary>
        /// Initializes a new instance of the OperationFailedException class with serialized data.
        /// </summary>
        /// <param name="info">The System.Runtime.Serialization.SerializationInfo that holds the serialized object data about the exception being thrown.</param>
        /// <param name="context">The System.Runtime.Serialization.StreamingContext that contains contextual information about the source or destination.</param>
        protected OperationFailedException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            ScheduleId = info.GetInt32(nameof(ScheduleId));
            InstanceId = info.GetString(nameof(InstanceId));
            OperationId = info.GetString(nameof(OperationId));
        }

        /// <inheritdoc />
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue(nameof(ScheduleId), ScheduleId);
            info.AddValue(nameof(InstanceId), InstanceId);
            info.AddValue(nameof(OperationId), OperationId);
        }

        /// <summary>
        /// Schedule Id of the exception
        /// </summary>
        public int ScheduleId { get; set; }

        /// <summary>
        /// The instance id of the entity that experienced the failure when executing the operation.
        /// </summary>
        public string InstanceId { get; set; }

        /// <summary>
        /// The operation id of the operation that experienced the failure.
        /// </summary>
        public string OperationId { get; set; }
    }
}