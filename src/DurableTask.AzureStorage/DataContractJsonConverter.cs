using Newtonsoft.Json;
using System;
using System.IO;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;

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

            using (var stream = new MemoryStream())
            using (var writer = new StreamWriter(stream))
            using (var jsonWriter = new JsonTextWriter(writer))
            {
                jsonWriter.WriteToken(reader, writeChildren: true);
                jsonWriter.Flush();
                stream.Position = 0;

                var contractSerializer = new DataContractJsonSerializer(objectType);
                return contractSerializer.ReadObject(stream);
            }
        }

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

            using (var memoryStream = new MemoryStream())
            {
                var contractSerializer = new DataContractJsonSerializer(value.GetType());
                contractSerializer.WriteObject(memoryStream, value);
                memoryStream.Position = 0;

                using (var streamReader = new StreamReader(memoryStream))
                using (var jsonReader = new JsonTextReader(streamReader))
                {
                    writer.WriteToken(jsonReader, writeChildren: true);
                }
            }
        }
    }
}
