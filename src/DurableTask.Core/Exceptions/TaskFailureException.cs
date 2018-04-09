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

namespace DurableTask.Core.Exceptions
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Exception type thrown by implementors of <see cref="TaskActivity"/> when exception
    /// details need to flow to parent orchestrations.
    /// </summary>
    [Serializable]
    public class TaskFailureException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TaskFailureException"/> class.
        /// </summary>
        public TaskFailureException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskFailureException"/> class.
        /// </summary>
        public TaskFailureException(string reason)
            : base(reason)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskFailureException"/> class.
        /// </summary>
        public TaskFailureException(string reason, Exception innerException)
            : base(reason, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskFailureException"/> class.
        /// </summary>
        public TaskFailureException(string reason, Exception innerException, string details)
            : base(reason, innerException)
        {
            this.Details = details;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskFailureException"/> class.
        /// </summary>
        public TaskFailureException(string reason, string details)
            : base(reason)
        {
            this.Details = details;
        }

        internal TaskFailureException WithFailureSource(string failureSource)
        {
            this.FailureSource = failureSource;
            return this;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskFailureException"/> class.
        /// </summary>
        protected TaskFailureException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            this.Details = info.GetString(Details);
            this.FailureSource = info.GetString(FailureSource);
        }

        /// <summary>
        /// Gets object data for use by serialization.
        /// </summary>
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue(Details, this.Details);
            info.AddValue(FailureSource, this.FailureSource);
        }

        /// <summary>
        /// Returns a debug string representing the current exception object.
        /// </summary>
        public override string ToString()
        {
            return string.Format("FailureSource: {1}{0}Details: {2}{0}Message: {3}{0}Exception: {4}", 
                Environment.NewLine,
                this.FailureSource,
                this.Details,
                this.Message,
                base.ToString());
        }

        /// <summary>
        /// Details of the exception which will flow to the parent orchestration.
        /// </summary>
        public string Details { get; set; }

        internal string FailureSource { get; set; }
    }
}