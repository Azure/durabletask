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
using System;

namespace DurableTask.Core.Entities.OperationFormat
{
    /// <summary>
    /// A response message sent by an entity to a caller after it executes an operation.
    /// </summary>
    public class OperationResult
    {
        // NOTE: Actions must be serializable by a variety of different serializer types to support out-of-process execution.
        //       To ensure maximum compatibility, all properties should be public and settable by default.

        /// <summary>
        /// The serialized result returned by the operation. Can be null, if the operation returned no result.
        /// May contain error details, such as a serialized exception, if <see cref="IsError"/> is true.
        /// </summary>
        public string? Result { get; set; }

        /// <summary>
        /// Whether this operation completed successfully.
        /// </summary>
        public bool IsError
            => this.ErrorMessage != null || this.FailureDetails != null;

        /// <summary>
        /// If non-null, this string indicates that this operation did not successfully complete. 
        /// The content and interpretation varies depending on the SDK used. For newer SDKs,
        /// we rely on the <see cref="FailureDetails"/> instead.
        /// </summary>
        public string? ErrorMessage { get; set; }

        /// <summary>
        /// A structured language-independent representation of the error. Whether this field is present
        /// depends on which SDK is used, and on configuration settings. For newer SDKs, we use
        /// this field exclusively when collecting error information.
        /// </summary>
        public FailureDetails? FailureDetails { get; set; }

        /// <summary>
        /// The start time of the operation.
        /// </summary>
        public DateTime? StartTimeUtc { get; set; }

        /// <summary>
        /// The completion time of the operation.
        /// </summary>
        public DateTime? EndTimeUtc { get; set; }
    }
}
