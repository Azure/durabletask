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
    using System.Collections.Generic;
    using System.Text;

    /// <summary>
    /// A class that includes an exception and correlation information for sending telemetry
    /// </summary>
    public class CorrelatedException
    {
        /// <summary>
        /// Exception that is sent to E2E tracing system
        /// </summary>
        public Exception Exception { get; set; }

        /// <summary>
        /// OperationId is uniq id of end to end tracing
        /// </summary>
        public string OperationId { get; set; }

        /// <summary>
        /// ParentId is an id of an end to end tracing
        /// </summary>
        public string ParentId { get; set; }

        /// <summary>
        /// A constructor with mandatory parameters.
        /// </summary>
        /// <param name="exception">Exception</param> 
        /// <param name="operationId">OperationId</param>
        /// <param name="parentId">ParentId</param>
        public CorrelatedException(Exception exception, string operationId, string parentId)
        {
            this.Exception = exception;
            this.ParentId = parentId;
            this.OperationId = operationId;
        }
    }
}
