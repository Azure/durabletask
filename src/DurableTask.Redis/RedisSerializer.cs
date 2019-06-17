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

using System.Collections.Generic;
using System.Linq;
using DurableTask.Core;
using DurableTask.Core.History;
using Newtonsoft.Json;

namespace DurableTask.Redis
{
    /// <summary>
    /// Serializes objects in a consistent matter so data can be stored and retrieved from Redis
    /// </summary>
    internal class RedisSerializer
    {
        private static readonly JsonSerializerSettings serializerSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.All
        };

        public static string SerializeObject(object obj)
        {
            return JsonConvert.SerializeObject(obj, serializerSettings);
        }

        public static T DeserializeObject<T>(string serializedObj)
        {
            return JsonConvert.DeserializeObject<T>(serializedObj, serializerSettings);
        }

        public static OrchestrationRuntimeState DeserializeRuntimeState(string serializedRuntimeState)
        {
            // OrchestrationRuntimeEvent builds its internal state with it's constructor and the AddEvent() method.
            // Must emulate that when deserializing
            IList<HistoryEvent> events = JsonConvert.DeserializeObject<IList<HistoryEvent>>(serializedRuntimeState, serializerSettings);
            return new OrchestrationRuntimeState(events);
        }
    }
}
