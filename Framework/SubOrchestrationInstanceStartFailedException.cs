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

namespace DurableTask
{
    using System;
    using System.Runtime.Serialization;
    using DurableTask.History;

    [Serializable]
    public class SubOrchestrationInstanceStartFailedException : OrchestrationException
    {
        public SubOrchestrationInstanceStartFailedException()
        {
        }

        public SubOrchestrationInstanceStartFailedException(string reason)
            : base(reason)
        {
        }

        public SubOrchestrationInstanceStartFailedException(string reason, Exception innerException)
            : base(reason, innerException)
        {
        }

        public SubOrchestrationInstanceStartFailedException(int eventId, int scheduleId, string name, string version, 
            OrchestrationInstanceStartFailureCause cause)
            : base(eventId, cause.ToString(), null)
        {
            ScheduleId = scheduleId;
            Name = name;
            Version = version;
            Cause = cause;
        }

        protected SubOrchestrationInstanceStartFailedException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public int ScheduleId { get; set; }
        public string Name { get; set; }
        public string Version { get; set; }
        public OrchestrationInstanceStartFailureCause Cause { get; set; }
    }
}