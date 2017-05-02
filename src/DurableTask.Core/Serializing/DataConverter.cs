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

namespace DurableTask.Core.Serializing
{
    using System;

    /// <summary>
    /// Abstract class for serializing and deserializing data
    /// </summary>
    public abstract class DataConverter
    {
        /// <summary>
        /// Serialize an Object to string with default formatting
        /// </summary>
        /// <param name="value">Object to serialize</param>
        /// <returns>Object serialized to a string</returns>
        public abstract string Serialize(object value);

        /// <summary>
        /// Serialize an Object to string with supplied formatting
        /// </summary>
        /// <param name="value">Object to serialize</param>
        /// <param name="formatted">Boolean indicating whether to format the results or not</param>
        /// <returns>Object serialized to a string</returns>
        public abstract string Serialize(object value, bool formatted);

        /// <summary>
        /// Deserialize a string to an Object of supplied type
        /// </summary>
        /// <param name="data">String data of the Object to deserialize</param>
        /// <param name="objectType">Type to deserialize to</param>
        /// <returns>Deserialized Object</returns>
        public abstract object Deserialize(string data, Type objectType);

        /// <summary>
        /// Deserialize a string to an Object of supplied type
        /// </summary>
        /// <param name="data">String data of the Object to deserialize</param>
        /// <typeparam name="T">Type to deserialize to</typeparam>
        /// <returns>Deserialized Object</returns>
        public T Deserialize<T>(string data)
        {
            return (T) Deserialize(data, typeof (T));
        }
    }
}