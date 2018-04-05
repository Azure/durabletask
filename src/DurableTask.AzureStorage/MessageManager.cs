namespace DurableTask.AzureStorage
{
    using System;
    using System.Collections.Generic;
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
        readonly CloudBlobContainer cloudBlobContainer;
        readonly JsonSerializerSettings taskMessageSerializerSettings;
        const string DefaultContainerName = "durable-compressedmessage-container";
        const int MaxStorageQueuePayloadSizeInBytes = 60 * 1024; // 60KB
        const int DefaultBufferSize = 64 * 2014; // 64KB

        /// <summary>
        /// The message manager.
        /// </summary>
        public MessageManager(CloudBlobClient cloudBlobClient)
        {
            this.cloudBlobContainer = cloudBlobClient.GetContainerReference(DefaultContainerName);
            this.cloudBlobContainer.CreateIfNotExistsAsync();
            this.taskMessageSerializerSettings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Objects
            };
        }

        /// <summary>
        /// Serializes the MessageData object
        /// </summary>
        public async Task<string> SerializeMessageDataAsync(MessageData messageData)
        {
            string rawContent = JsonConvert.SerializeObject(messageData, this.taskMessageSerializerSettings);
            messageData.TotalMessageSizeBytes = Encoding.Unicode.GetByteCount(rawContent);
            MessageFormatFlags messageFormat = this.GetMessageFormatFlags(messageData);

            if (messageFormat != MessageFormatFlags.InlineJson)
            {
                byte[] messageBytes = Encoding.Unicode.GetBytes(rawContent);

                // Get Compressed bytes
                string blobName = messageData.ActivityId.ToString();
                await this.CompressAndUploadAsBytesAsync(messageBytes, blobName);

                MessageData wrapperMessageData = new MessageData
                {
                    CompressedBlobName = blobName
                };

                return JsonConvert.SerializeObject(wrapperMessageData, this.taskMessageSerializerSettings);
            }

            return JsonConvert.SerializeObject(messageData, this.taskMessageSerializerSettings);
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
                string decompressedMessage = await this.DownloadAndDecompressAsBytesAsync(envelope.CompressedBlobName);
                envelope = JsonConvert.DeserializeObject<MessageData>(
                    decompressedMessage,
                    this.taskMessageSerializerSettings);
                envelope.MessageFormat = MessageFormatFlags.StorageBlob;
            }

            envelope.OriginalQueueMessage = queueMessage;
            envelope.TotalMessageSizeBytes = Encoding.Unicode.GetByteCount(queueMessage.AsString);
            envelope.QueueName = queueName;
            return envelope;
        }

        internal async Task<string> CompressAndUploadAsBytesAsync(byte[] payloadBuffer, string activationId)
        {
            byte[] compressedBytes = this.Compress(payloadBuffer);

            return await this.UploadToBlobAsync(compressedBytes, compressedBytes.Length, activationId);
        }

        internal byte[] Compress(byte[] payloadBuffer)
        {
            using (var originStream = new MemoryStream(payloadBuffer, 0, payloadBuffer.Length))
            {
                using (MemoryStream memory = new MemoryStream())
                {
                    using (GZipStream gzip = new GZipStream(memory, CompressionLevel.Optimal))
                    {
                        byte[] buffer = SharedBufferManager.Instance.TakeBuffer(DefaultBufferSize);
                        try
                        {
                            int read;
                            while ((read = originStream.Read(buffer, 0, DefaultBufferSize)) != 0)
                            {
                                gzip.Write(buffer, 0, read);
                            }

                            gzip.Flush();
                        }
                        finally
                        {
                            SharedBufferManager.Instance.ReturnBuffer(buffer);
                        }

                    }
                    return memory.ToArray();
                }
            }
        }

        internal async Task<string> DownloadAndDecompressAsBytesAsync(string blobName)
        {
            byte[] downloadBlobAsAsBytes = await this.DownloadBlobAsync(blobName);
            byte[] decompressedBytes = this.Decompress(downloadBlobAsAsBytes);
            return Encoding.Unicode.GetString(decompressedBytes);
        }

        internal byte[] Decompress(byte[] gzip)
        {
            using (GZipStream stream = new GZipStream(new MemoryStream(gzip), CompressionMode.Decompress))
            {
                using (MemoryStream memory = new MemoryStream())
                {
                    byte[] buffer = SharedBufferManager.Instance.TakeBuffer(DefaultBufferSize);

                    try
                    {
                        int count = 0;
                        do
                        {
                            count = stream.Read(buffer, 0, DefaultBufferSize);
                            if (count > 0)
                            {
                                memory.Write(buffer, 0, count);
                            }
                        }
                        while (count > 0);

                    }
                    finally
                    {
                        SharedBufferManager.Instance.ReturnBuffer(buffer);
                    }
                    return memory.ToArray();
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
        internal async Task<string> UploadToBlobAsync(byte[] data, int dataByteCount, string blobName)
        {
            CloudBlockBlob cloudBlockBlob = this.cloudBlobContainer.GetBlockBlobReference(blobName);
            await cloudBlockBlob.UploadFromByteArrayAsync(data, 0, dataByteCount);
            return blobName;
        }

        /// <summary>
        /// Downloads MessageData as bytes[] 
        /// </summary>
        internal async Task<byte[]> DownloadBlobAsync(string blobName)
        {
            CloudBlockBlob cloudBlockBlob = this.cloudBlobContainer.GetBlockBlobReference(blobName);

            using (MemoryStream ms = new MemoryStream())
            {
                await cloudBlockBlob.DownloadToStreamAsync(ms);
                return ms.ToArray();
            }
        }
    }
}
