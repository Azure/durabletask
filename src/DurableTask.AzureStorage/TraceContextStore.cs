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

namespace DurableTask.AzureStorage
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using DurableTask.Core;
    using Newtonsoft.Json;

    /// <summary>
    /// TraceContextStore store the Json representation of the TraceContext
    /// TraceContext requires circular reference dependency. It can Serialize/Deserialize by the latest NewtonJon.
    /// DurableTask support both .Net Framework 4.5.1. with Newton.json 7.0.1 and .Net Standard 2.1. with Newton.json 11.0.1
    /// For the Newton.json 7.0.1 requires JsonSerializerSettings. It will all so requires DurableTask users.
    /// To avoid this breaking change, I provide TraceContextStore which support the serialization/deserialization of the <see cref="TraceContext"/>
    /// </summary>
    public class TraceContextStore
    {
        /// <summary>
        /// Store Json representation of the TraceContext
        /// </summary>
        public string TraceContextJson { get; set; }

        /// <summary>
        /// Restore TraceContext instance
        /// </summary>
        /// <returns></returns>
        public TraceContext Restore()
        {
            return JsonConvert.DeserializeObject<TraceContext>(TraceContextJson);
        }

        /// <summary>
        /// Create a TraceContextStore object from the TraceContext object.
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public static TraceContextStore Create(TraceContext context)
        {
            var store = new TraceContextStore();
            string json = JsonConvert.SerializeObject(context, new JsonSerializerSettings()
            {
                TypeNameHandling = TypeNameHandling.Objects,
                PreserveReferencesHandling = PreserveReferencesHandling.Objects,
                ReferenceLoopHandling = ReferenceLoopHandling.Serialize
            });
            store.TraceContextJson = json;
            return store;
        }
    }
}
