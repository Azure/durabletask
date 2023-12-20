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
#nullable enable
namespace DurableTask.Core.Entities
{
    using Newtonsoft.Json;

    internal static class Serializer
    {
        /// <summary>
        /// This serializer is used exclusively for internally defined data structures and cannot be customized by user.
        /// This is intentional, to avoid problems caused by our unability to control the exact format.
        /// For example, including typenames can cause compatibility problems if the type name is later changed.
        /// </summary>
        public static JsonSerializer InternalSerializer = JsonSerializer.Create(InternalSerializerSettings);

        public static JsonSerializerSettings InternalSerializerSettings 
            = new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.None };
    }
}
