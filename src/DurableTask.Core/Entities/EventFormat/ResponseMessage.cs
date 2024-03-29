﻿//  ----------------------------------------------------------------------------------
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
namespace DurableTask.Core.Entities.EventFormat
{
    using System.Runtime.Serialization;

    [DataContract]
    internal class ResponseMessage : EntityMessage
    {
        public const string LockAcquisitionCompletion = "Lock Acquisition Completed";

        [DataMember(Name = "result")]
        public string? Result { get; set; }

        [DataMember(Name = "exceptionType", EmitDefaultValue = false)]
        public string? ErrorMessage { get; set; }

        [DataMember(Name = "failureDetails", EmitDefaultValue = false)]
        public FailureDetails? FailureDetails { get; set; }

        [IgnoreDataMember]
        public bool IsErrorResult => this.ErrorMessage != null || this.FailureDetails != null;

        public override string GetShortDescription()
        {
            if (this.IsErrorResult)
            {
                return $"[OperationFailed {this.FailureDetails?.ErrorMessage ?? this.ErrorMessage}]";
            }
            else if (this.Result == LockAcquisitionCompletion)
            {
                return "[LockAcquisitionComplete]";
            }
            else
            {
                return $"[OperationSuccessful ({Result?.Length ?? 0} chars)]";
            }
        }
    }
}
