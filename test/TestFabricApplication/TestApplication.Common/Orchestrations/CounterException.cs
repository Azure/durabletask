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
        readonly string CounterPropName = "Counter";

        public CounterException(int counter)
        {
            this.Counter = counter;
        }

        protected CounterException(SerializationInfo info, StreamingContext context)
        {
            this.Counter = info.GetInt32(CounterPropName);
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue(CounterPropName, this.Counter);
            base.GetObjectData(info, context);
        }

        public int Counter { get; }
    }
}
