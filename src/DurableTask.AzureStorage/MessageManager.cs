﻿//  ----------------------------------------------------------------------------------
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
    using System.IO;
    using System.IO.Compression;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Queue;
    using Newtonsoft.Json;

    /// <summary>
    /// The message manager for messages from MessageData, and DynamicTableEntities
    /// </summary>
    public class MessageManager
    {
        const int MaxStorageQueuePayloadSizeInBytes = 60 * 1024; // 60KB
        const int DefaultBufferSize = 64 * 2014; // 64KB
        const string LargeMessagesContainerPrefix = "largemessages-";
        const string LargeMessageBlobNameSeparator = "/";

        readonly CloudBlobClient cloudBlobClient;
        readonly JsonSerializerSettings taskMessageSerializerSettings;

        /// <summary>
        /// The message manager.
        /// </summary>
        public MessageManager(CloudBlobClient cloudBlobClient)
        {
            this.cloudBlobClient = cloudBlobClient;
            this.taskMessageSerializerSettings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Objects
            };
        }

        /// <summary>
        /// Serializes the MessageData object
        /// </summary>
        /// <param name="messageData">Instance of <see cref="MessageData"/></param>
        /// <param name="instanceId">Instance ID</param>
        /// <returns>Instance of Tuple mapping instance of <see cref="MessageData"/> and blob name</returns>
        public async Task<Tuple<MessageData, string>> SerializeMessageDataAsync(MessageData messageData, string instanceId)
        {
            string rawContent = JsonConvert.SerializeObject(messageData, this.taskMessageSerializerSettings);
            messageData.TotalMessageSizeBytes = Encoding.Unicode.GetByteCount(rawContent);
            MessageFormatFlags messageFormat = this.GetMessageFormatFlags(messageData);

            if (messageFormat != MessageFormatFlags.InlineJson)
            {
                byte[] messageBytes = Encoding.UTF8.GetBytes(rawContent);

                // Get Compressed bytes
                string blobName = Guid.NewGuid().ToString();
                await this.CompressAndUploadAsBytesAsync(messageBytes, instanceId, blobName);
                string fullBlobName = this.GetFullLargeMessageBlobName(instanceId, blobName);
                MessageData wrapperMessageData = new MessageData
                {
                    CompressedBlobName = fullBlobName
                };

                messageData.CompressedBlobName = fullBlobName;

                return new Tuple<MessageData, string>(messageData, JsonConvert.SerializeObject(wrapperMessageData, this.taskMessageSerializerSettings));
            }

            return new Tuple<MessageData, string>(messageData, JsonConvert.SerializeObject(messageData, this.taskMessageSerializerSettings));
        }

        /// <summary>
        /// Deserializes the MessageData object
        /// </summary>
        public async Task<MessageData> DeserializeQueueMessageAsync(CloudQueueMessage queueMessage, string queueName)
        {
            MessageData envelope = JsonConvert.DeserializeObject<MessageData>(
                queueMessage.AsString,
                this.taskMessageSerializerSettings);

            if (!string.IsNullOrEmpty(envelope.CompressedBlobName))
            {
                var compressedBlobName = envelope.CompressedBlobName;
                string decompressedMessage = await this.DownloadAndDecompressAsBytesAsync(envelope.CompressedBlobName);
                envelope = JsonConvert.DeserializeObject<MessageData>(
                    decompressedMessage,
                    this.taskMessageSerializerSettings);
                envelope.MessageFormat = MessageFormatFlags.StorageBlob;
                envelope.CompressedBlobName = compressedBlobName;
            }

            envelope.OriginalQueueMessage = queueMessage;
            envelope.TotalMessageSizeBytes = Encoding.Unicode.GetByteCount(queueMessage.AsString);
            envelope.QueueName = queueName;
            return envelope;
        }

        internal Task CompressAndUploadAsBytesAsync(byte[] payloadBuffer, string instanceId, string blobName)
        {
            ArraySegment<byte> compressedSegment = this.Compress(payloadBuffer);
            return this.UploadToBlobAsync(compressedSegment.Array, compressedSegment.Count, instanceId, blobName);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2202:DoNotDisposeObjectsMultipleTimes", Justification = "This GZipStream will not dispose the MemoryStream.")]
        internal ArraySegment<byte> Compress(byte[] payloadBuffer)
        {
            using (var originStream = new MemoryStream(payloadBuffer, 0, payloadBuffer.Length))
            {
                using (MemoryStream memory = new MemoryStream())
                {
                    using (GZipStream gZipStream = new GZipStream(memory, CompressionLevel.Optimal, leaveOpen: true))
                    {
                        byte[] buffer = SimpleBufferManager.Shared.TakeBuffer(DefaultBufferSize);
                        try
                        {
                            int read;
                            while ((read = originStream.Read(buffer, 0, DefaultBufferSize)) != 0)
                            {
                                gZipStream.Write(buffer, 0, read);
                            }

                            gZipStream.Flush();
                        }
                        finally
                        {
                            SimpleBufferManager.Shared.ReturnBuffer(buffer);
                        }
                    }

                    return new ArraySegment<byte>(memory.GetBuffer(), 0, (int)memory.Length);
                }
            }
        }

        internal async Task<string> DownloadAndDecompressAsBytesAsync(string fullBlobName)
        {
            string[] blobNameParts = fullBlobName.Split(new[] { LargeMessageBlobNameSeparator }, StringSplitOptions.None);
            if (blobNameParts.Length != 2)
            {
                return string.Empty;
            }

            string containerName = blobNameParts[0];
            string blobName = blobNameParts[1];

            CloudBlobContainer cloudBlobContainer =
                this.cloudBlobClient.GetContainerReference(containerName);
            CloudBlockBlob cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference(blobName);
            Stream downloadBlobAsStream = await cloudBlockBlob.OpenReadAsync();
            ArraySegment<byte> decompressedSegment = this.Decompress(downloadBlobAsStream);
            return Encoding.UTF8.GetString(decompressedSegment.Array, 0, decompressedSegment.Count);
        }

        internal ArraySegment<byte> Decompress(Stream blobStream)
        {
            using (GZipStream gZipStream = new GZipStream(blobStream, CompressionMode.Decompress))
            {
                using (MemoryStream memory = new MemoryStream(MaxStorageQueuePayloadSizeInBytes * 2))
                {
                    byte[] buffer = SimpleBufferManager.Shared.TakeBuffer(DefaultBufferSize);
                    try
                    {
                        int count = 0;
                        while ((count = gZipStream.Read(buffer, 0, DefaultBufferSize)) > 0)
                        {
                            memory.Write(buffer, 0, count);
                        }
                    }
                    finally
                    {
                        SimpleBufferManager.Shared.ReturnBuffer(buffer);
                    }

                    return new ArraySegment<byte>(memory.GetBuffer(), 0, (int)memory.Length);
                }
            }
        }

        internal MessageFormatFlags GetMessageFormatFlags(MessageData messageData)
        {
            MessageFormatFlags messageFormatFlags = MessageFormatFlags.InlineJson;

            if (messageData.TotalMessageSizeBytes > MaxStorageQueuePayloadSizeInBytes)
            {
                messageFormatFlags = MessageFormatFlags.StorageBlob;
            }

            return messageFormatFlags;
        }

        /// <summary>
        /// Uploads MessageData as bytes[] to blob container
        /// </summary>
        internal async Task UploadToBlobAsync(byte[] data, int dataByteCount, string instanceId, string blobName)
        {
            CloudBlobContainer cloudBlobContainer = this.GetLargeMessagesContainer(instanceId);
            await cloudBlobContainer.CreateIfNotExistsAsync();
            CloudBlockBlob cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference(blobName);
            await cloudBlockBlob.UploadFromByteArrayAsync(data, 0, dataByteCount);
        }

        internal Task DeleteLargeMessagesContainer(string instanceId)
        {
            CloudBlobContainer cloudBlobContainer = this.GetLargeMessagesContainer(instanceId);
            return cloudBlobContainer.DeleteIfExistsAsync();
        }

        private CloudBlobContainer GetLargeMessagesContainer(string instanceId)
        {
            var containerNameBuilder = new StringBuilder();
            containerNameBuilder.Append(LargeMessagesContainerPrefix).Append(instanceId.ToLowerInvariant());
            string compressedMessageBlobContainerName = containerNameBuilder.ToString();
            NameValidator.ValidateContainerName(compressedMessageBlobContainerName);
            CloudBlobContainer cloudBlobContainer = 
                this.cloudBlobClient.GetContainerReference(compressedMessageBlobContainerName);
            return cloudBlobContainer;
        }

        internal string GetFullLargeMessageBlobName(string instanceId, string blobName)
        {
            var blobNameBuilder = new StringBuilder();
            blobNameBuilder.Append(LargeMessagesContainerPrefix).Append(instanceId.ToLowerInvariant()).Append(LargeMessageBlobNameSeparator).Append(blobName);
            return blobNameBuilder.ToString();
        }
    }
}
