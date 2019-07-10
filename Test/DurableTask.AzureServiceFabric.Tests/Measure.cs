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
    using System.Diagnostics;
    using System.IO;
    using System.Runtime.Serialization;

    class Measure
    {
        public static T DataContractSerialization<T>(T testObject)
        {
            return (T)DataContractSerialization(typeof(T), testObject);
        }

        public static object DataContractSerialization(Type testType, object testObject)
        {
            using (var stream = new MemoryStream())
            {
                var serializer = new DataContractSerializer(testType);
                var time = MeasureTime(() => serializer.WriteObject(stream, testObject));
                Console.WriteLine($"Time for serialization : {time.TotalMilliseconds} ms");
                Console.WriteLine($"Size of serialized stream : {stream.Length} bytes.");

                stream.Position = 0;
                object deserialized = null;
                time = MeasureTime(() => { deserialized = serializer.ReadObject(stream); });
                Console.WriteLine($"Time for deserialization : {time.TotalMilliseconds} ms");
                return deserialized;
            }
        }

        public static TimeSpan MeasureTime(Action action)
        {
            Stopwatch timer = Stopwatch.StartNew();
            action();
            timer.Stop();
            return timer.Elapsed;
        }
    }
}
