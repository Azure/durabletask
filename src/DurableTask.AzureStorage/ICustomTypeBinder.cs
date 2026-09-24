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

using System;

namespace DurableTask.AzureStorage
{
    /// <summary>
    /// Abstraction to prevent surfacing the dynamic loading between System.Runtime.Serialization.SerializationBinder and Newtonsoft.Json.Serialization.ISerializationBinder
    /// Used when serializing and deserializing queue messages to allow providing custom type binding.
    /// Custom bindings for DurableTask types are not used during deserialization.
    /// </summary>
    public interface ICustomTypeBinder
    {
        /// <summary>
        /// When implemented, supplies the assembly and type names written during serialization.
        /// </summary>
        /// <remarks>
        /// Return stable names for a given type. The number of invocations is an implementation detail
        /// and can vary with the message size, storage path, and serializer configuration.
        /// Implementations must not depend on a particular invocation count for correctness.
        /// </remarks>
        /// <param name="serializedType">The type of the object being serialized.</param>
        /// <param name="assemblyName">Specifies the System.Reflection.Assembly name of the serialized object.</param>
        /// <param name="typeName">Specifies the System.Type name of the serialized object.</param>
        void BindToName(Type serializedType, out string assemblyName, out string typeName);

        /// <summary>
        /// When implemented, controls the binding of a serialized object to a type.
        /// </summary>
        /// <param name="assemblyName">Specifies the System.Reflection.Assembly name of the serialized object.</param>
        /// <param name="typeName">Specifies the System.Type name of the serialized object</param>
        /// <returns>The type of the object the formatter creates a new instance of.</returns>
        Type BindToType(string assemblyName, string typeName);
    }
}