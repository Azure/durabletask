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

    [Serializable]
    internal class TaskFailureException : Exception
    {
        public TaskFailureException()
        {
        }

        public TaskFailureException(string reason)
            : base(reason)
        {
        }

        public TaskFailureException(string reason, Exception innerException)
            : base(reason, innerException)
        {
        }

        public TaskFailureException(string reason, Exception innerException, string details)
            : base(reason, innerException)
        {
            this.Details = details;
        }

        public TaskFailureException(string reason, string details)
            : base(reason)
        {
            this.Details = details;
        }

        public TaskFailureException WithFailureSource(string failureSource)
        {
            this.FailureSource = failureSource;
            return this;
        }

        protected TaskFailureException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            this.Details = info.GetString(Details);
            this.FailureSource = info.GetString(FailureSource);
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue(Details, this.Details);
            info.AddValue(FailureSource, this.FailureSource);
        }

        public override string ToString()
        {
            return string.Format("FailureSource: {1}{0}Details: {2}{0}Message: {3}{0}Exception: {4}", 
                Environment.NewLine,
                this.FailureSource,
                this.Details,
                this.Message,
                base.ToString());
        }

        public string Details { get; set; }

        public string FailureSource { get; set; }
    }
}