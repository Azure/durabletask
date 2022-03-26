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

namespace DurableTask.Core
{
    using System;
    using DurableTask.Core.Exceptions;

    /// <summary>
    /// Specifies the mechanism for propagating unhandled exception information from tasks to their parent orchestrations.
    /// </summary>
    public enum ErrorPropagationMode
    {
        /// <summary>
        /// Unhandled exceptions are serialized and surfaced to callers via the <see cref="Exception.InnerException"/>
        /// property of <see cref="TaskFailedException"/>. This is the legacy behavior and doesn't work for all exception types.
        /// </summary>
        SerializeExceptions = 0,

        /// <summary>
        /// Details of unhandled exceptions are surfaced via the <see cref="OrchestrationException.FailureDetails"/> property.
        /// The original exception objects are not serialized and deserialized across tasks. This option is preferred
        /// when consistency is required or in advanced scenarios where orchestration or activity code are executed
        /// out of process, potentially in other language runtimes that don't support .NET/CLR exceptions.
        /// </summary>
        UseFailureDetails = 1,
    }
}
