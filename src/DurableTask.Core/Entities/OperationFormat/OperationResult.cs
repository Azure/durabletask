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
        /// May contain error details, such as a serialized exception, if <see cref="ErrorMessage"/> is not null.
        /// </summary>
        public string? Result { get; set; }

        /// <summary>
        /// If non-null, this string indicates that this operation did not successfully complete. 
        /// The actual content and its interpretation varies depending on the SDK used. 
        /// </summary>
        public string? ErrorMessage { get; set; }

        /// <summary>
        /// A structured language-independent representation of the error. Whether this field is present
        /// depends on which SDK is used, and on configuration settings.
        /// </summary>
        public FailureDetails? FailureDetails { get; set; }
    }
}
