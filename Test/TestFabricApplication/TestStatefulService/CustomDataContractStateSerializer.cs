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


namespace TestStatefulService
{
    using System.IO;
    using System.Runtime.Serialization;
    using System.Xml;
    using Microsoft.ServiceFabric.Data;

    internal class CustomDataContractStateSerializer<T> : IStateSerializer<T>
    {
        /// <summary>
        /// The serializer.
        /// </summary>
        private readonly DataContractSerializer serializer = new DataContractSerializer(typeof(T));

        /// <summary>
        /// Converts byte[] to T
        /// </summary>
        /// <param name="binaryReader">Reader containing the serialized data.</param>
        /// <returns>Deserialized version of T.</returns>
        public T Read(BinaryReader binaryReader)
        {
            var size = binaryReader.ReadInt32();

            var bytes = binaryReader.ReadBytes(size);

            using (var memoryStream = new MemoryStream(bytes))
            {
                using (var reader = XmlDictionaryReader.CreateBinaryReader(memoryStream, XmlDictionaryReaderQuotas.Max))
                using (var customReader = new CustomerXmlDictionaryReader(reader, typeof(T)))
                {
                    return (T) this.serializer.ReadObject(customReader);
                }
            }
        }

        /// <summary>
        /// Converts IEnumerable of byte[] to T
        /// </summary>
        /// <param name="baseValue"></param>
        /// <param name="reader">Reader containing the serialized data.</param>
        /// <returns>Deserialized version of T.</returns>
        public T Read(T baseValue, BinaryReader reader)
        {
            return this.Read(reader);
        }

        /// <summary>
        /// Converts T to byte array.
        /// </summary>
        /// <param name="value">T to be serialized.</param>
        /// <param name="binaryWriter"></param>
        /// <returns>Serialized version of T.</returns>
        public void Write(T value, BinaryWriter binaryWriter)
        {
            using (var memoryStream = new MemoryStream())
            {
                using (var innerWriter = new BinaryWriter(memoryStream))
                {
                    innerWriter.Write(int.MinValue);

                    using (
                        var binaryDictionaryWriter = XmlDictionaryWriter.CreateBinaryWriter(
                            memoryStream,
                            null,
                            null,
                            false))
                    {
                        this.serializer.WriteObject(binaryDictionaryWriter, value);
                        binaryDictionaryWriter.Flush();
                    }

                    var lastPosition = (int) memoryStream.Position;

                    memoryStream.Position = 0;

                    innerWriter.Write(lastPosition - sizeof(int));

                    memoryStream.Position = lastPosition;

                    binaryWriter.Write(memoryStream.GetBuffer(), 0, lastPosition);
                }
            }
        }

        /// <summary>
        /// Converts T to byte array.
        /// </summary>
        /// <param name="newValue"></param>
        /// <param name="binaryWriter">Writer to which the serialized data should be written.</param>
        /// <param name="currentValue"></param>
        /// <returns>Serialized version of T.</returns>
        public void Write(T currentValue, T newValue, BinaryWriter binaryWriter)
        {
            this.Write(newValue, binaryWriter);
        }
    }
}