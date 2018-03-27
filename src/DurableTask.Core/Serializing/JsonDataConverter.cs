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
    using System.Globalization;
    using System.IO;
    using System.Text;
    using Newtonsoft.Json;

    /// <summary>
    /// Class for serializing and deserializing data to and from json
    /// </summary>
    public class JsonDataConverter : DataConverter
    {
        readonly JsonSerializer serializer;

        /// <summary>
        /// Creates a new instance of the JsonDataConverter with default settings
        /// </summary>
        public JsonDataConverter()
            : this(new JsonSerializerSettings {TypeNameHandling = TypeNameHandling.Objects})
        {
        }

        /// <summary>
        /// Creates a new instance of the JsonDataConverter with supplied settings
        /// </summary>
        /// <param name="settings">Settings for the json serializer</param>
        public JsonDataConverter(JsonSerializerSettings settings)
        {
            serializer = JsonSerializer.Create(settings);
        }

        /// <summary>
        /// Serialize an Object to string with default formatting
        /// </summary>
        /// <param name="value">Object to serialize</param>
        /// <returns>Object serialized to a string</returns>
        public override string Serialize(object value)
        {
            return Serialize(value, false);
        }

        /// <summary>
        /// Serialize an Object to string with supplied formatting
        /// </summary>
        /// <param name="value">Object to serialize</param>
        /// <param name="formatted">Boolean indicating whether to format the results or not</param>
        /// <returns>Object serialized to a string</returns>
        public override string Serialize(object value, bool formatted)
        {
            var sb = new StringBuilder(0x100);
            var textWriter = new StringWriter(sb, CultureInfo.InvariantCulture);
            using (var writer = new JsonTextWriter(textWriter))
            {
                writer.Formatting = (formatted ? Formatting.Indented : Formatting.None);
                serializer.Serialize(writer, value);
            }

            return textWriter.ToString();
        }

        /// <summary>
        /// Deserialize a string to an Object of supplied type
        /// </summary>
        /// <param name="data">String data of the Object to deserialize</param>
        /// <param name="objectType">Type to deserialize to</param>
        /// <returns>Deserialized Object</returns>
        public override object Deserialize(string data, Type objectType)
        {
            if (data == null)
            {
                return null;
            }

            var reader = new StringReader(data);

            return serializer.Deserialize(new JsonTextReader(reader), objectType);
        }
    }
}