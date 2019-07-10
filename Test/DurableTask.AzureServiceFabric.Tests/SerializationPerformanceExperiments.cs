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

namespace DurableTask.AzureServiceFabric.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.Serialization;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class SerializationPerformanceExperiments
    {
        [TestMethod]
        public void DataContractSerializationSizeExperiment_PrimitiveTypes()
        {
            var testObjects = new List<object>()
            {
                Int32.MinValue,
                Int32.MaxValue,
                "01234567890123456789012345678901",
                Guid.NewGuid(),
                DateTime.UtcNow,
                Int64.MaxValue,
                Int64.MinValue,
                Int16.MaxValue,
                Int16.MinValue,
                new ComplexType() { LongProp = 33, StringProp = "0123456789" }
            };

            foreach (var testObject in testObjects)
            {
                var type = testObject.GetType();
                Console.WriteLine($"Type = {type.Name}");
                Console.WriteLine($"TestObject = {testObject}");
                Measure.DataContractSerialization(type, testObject);
                Console.WriteLine();
            }
        }

        [DataContract]
        class ComplexType
        {
            [DataMember] public long LongProp;

            [DataMember] public string StringProp;

            public override string ToString()
            {
                return $"'LongProp = {LongProp}, StringProp = {StringProp}'";
            }
        }
    }
}
