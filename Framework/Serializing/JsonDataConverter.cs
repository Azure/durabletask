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

namespace DurableTask
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Text;
    using Newtonsoft.Json;

    public class JsonDataConverter : DataConverter
    {
        readonly JsonSerializer serializer;

        public JsonDataConverter()
            : this(new JsonSerializerSettings {TypeNameHandling = TypeNameHandling.Objects})
        {
        }

        public JsonDataConverter(JsonSerializerSettings settings)
        {
            serializer = JsonSerializer.Create(settings);
        }

        public override string Serialize(object value)
        {
            var sb = new StringBuilder(0x100);
            var textWriter = new StringWriter(sb, CultureInfo.InvariantCulture);
            using (var writer = new JsonTextWriter(textWriter))
            {
                writer.Formatting = Formatting.None;
                serializer.Serialize(writer, value);
            }
            return textWriter.ToString();
        }

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