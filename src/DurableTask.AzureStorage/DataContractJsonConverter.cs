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
using System.IO;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;
using Newtonsoft.Json;

namespace DurableTask.AzureStorage
{
    /// <summary>
    /// This class bridges <see cref="DataContractJsonSerializer"/> with Newtonsoft.Json. This serializer
    /// is slower, but it handles writing to <see cref="IExtensibleDataObject"/>, which Newtonsoft does not.
    /// A drawback of <see cref="DataContractJsonSerializer"/> is that ExtensionData Namespaces are not populated,
    /// meaning reading via the regular <see cref="DataContractSerializer"/> will not correctly hydrate extra fields
    /// from ExtensionData. However, it can still be done by using <see cref="DataContractJsonSerializer"/> instead.
    /// </summary>
    internal class DataContractJsonConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            if (objectType == null)
            {
                throw new ArgumentNullException(nameof(objectType));
            }

            return objectType.GetCustomAttribute<DataContractAttribute>() != null
                && typeof(IExtensibleDataObject).IsAssignableFrom(objectType);
        }

        /// <inheritdoc />
        public override object ReadJson(
            JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader == null)
            {
                throw new ArgumentNullException(nameof(reader));
            }

            if (objectType == null)
            {
                throw new ArgumentNullException(nameof(objectType));
            }

            if (serializer == null)
            {
                throw new ArgumentNullException(nameof(serializer));
            }

            using (var stream = new MemoryStream())
            using (var writer = new StreamWriter(stream))
            using (var jsonWriter = new JsonTextWriter(writer))
            {
                jsonWriter.WriteToken(reader, writeChildren: true);
                jsonWriter.Flush();
                stream.Position = 0;

                var contractSerializer = CreateSerializer(objectType, serializer);
                return contractSerializer.ReadObject(stream);
            }
        }

        /// <inheritdoc />
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (writer == null)
            {
                throw new ArgumentNullException(nameof(writer));
            }

            if (value == null)
            {
                writer.WriteNull();
                return;
            }

            if (serializer == null)
            {
                throw new ArgumentNullException(nameof(serializer));
            }

            using (var memoryStream = new MemoryStream())
            {
                var contractSerializer = CreateSerializer(value.GetType(), serializer);
                contractSerializer.WriteObject(memoryStream, value);
                memoryStream.Position = 0;

                using (var streamReader = new StreamReader(memoryStream))
                using (var jsonReader = new JsonTextReader(streamReader))
                {
                    writer.WriteToken(jsonReader, writeChildren: true);
                }
            }
        }

        private static DataContractJsonSerializer CreateSerializer(Type type, JsonSerializer serializer)
        {
            return new DataContractJsonSerializer(
                type,
                new DataContractJsonSerializerSettings
                {
                    DateTimeFormat = new DateTimeFormat(serializer.DateFormatString),
                });
        }
    }
}
