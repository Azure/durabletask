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

namespace TestApplication.Common.Orchestrations
{
    using System;
    using System.Runtime.Serialization;

    [Serializable]
    public class CounterException : Exception
    {
        static readonly string CounterPropName = "Counter";

        public CounterException(int counter)
        {
            this.Counter = counter;
        }

        protected CounterException(SerializationInfo info, StreamingContext context)
        {
            this.Counter = info.GetInt32(CounterPropName);
        }

        /// <summary>Initializes a new instance of the <see cref="CounterException" /> class.</summary>
        CounterException() { }

        /// <summary>Initializes a new instance of the <see cref="CounterException" /> class with a specified error message.</summary>
        /// <param name="message">The message that describes the error.</param>
        CounterException(string message) : base(message) { }

        /// <summary>Initializes a new instance of the <see cref="CounterException" /> class with a specified error message and a reference to the inner exception that is the cause of this exception.</summary>
        /// <param name="message">The error message that explains the reason for the exception.</param>
        /// <param name="innerException">The exception that is the cause of the current exception, or a null reference (<see langword="Nothing" /> in Visual Basic) if no inner exception is specified.</param>
        CounterException(string message, Exception innerException) : base(message, innerException) { }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue(CounterPropName, this.Counter);
            base.GetObjectData(info, context);
        }

        public int Counter { get; }
    }
}
