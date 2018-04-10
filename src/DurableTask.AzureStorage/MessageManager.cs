﻿namespace DurableTask.AzureStorage
{
    using System;
    using System.IO;
    using System.IO.Compression;
    using System.Text;
    using System.Threading.Tasks;
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
        readonly SharedBufferManager sharedBufferManager;
        const int MaxStorageQueuePayloadSizeInBytes = 60 * 1024; // 60KB
        const int DefaultBufferSize = 64 * 2014; // 64KB

        /// <summary>
        /// The message manager.
        /// </summary>
        public MessageManager(CloudBlobClient cloudBlobClient, string blobContainerName)
        {
            this.cloudBlobContainer = cloudBlobClient.GetContainerReference(blobContainerName);
            this.taskMessageSerializerSettings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Objects
            };

            this.sharedBufferManager = new SharedBufferManager();
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
                byte[] messageBytes = Encoding.UTF8.GetBytes(rawContent);

                // Get Compressed bytes
                string blobName = Guid.NewGuid().ToString();
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

        internal async Task CompressAndUploadAsBytesAsync(byte[] payloadBuffer, string blobName)
        {
            byte[] compressedBytes = this.Compress(payloadBuffer);
            await this.UploadToBlobAsync(compressedBytes, compressedBytes.Length, blobName);
        }

        internal byte[] Compress(byte[] payloadBuffer)
        {
            using (var originStream = new MemoryStream(payloadBuffer, 0, payloadBuffer.Length))
            {
                using (MemoryStream memory = new MemoryStream())
                {
                    using (GZipStream gZipStream = new GZipStream(memory, CompressionLevel.Optimal))
                    {
                        byte[] buffer = this.sharedBufferManager.TakeBuffer(DefaultBufferSize);
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
                            this.sharedBufferManager.ReturnBuffer(buffer);
                        }

                        return new ArraySegment<byte>(memory.GetBuffer(), 0, (int)memory.Length).Array;
                    }
                }
            }
        }

        internal async Task<string> DownloadAndDecompressAsBytesAsync(string blobName)
        {
            CloudBlockBlob cloudBlockBlob = this.cloudBlobContainer.GetBlockBlobReference(blobName);
            Stream downloadBlobAsStream = await cloudBlockBlob.OpenReadAsync();
            byte[] decompressedBytes = this.Decompress(downloadBlobAsStream);
            return Encoding.UTF8.GetString(decompressedBytes);
        }

        internal byte[] Decompress(Stream blobStream)
        {
            using (GZipStream gZipStream = new GZipStream(blobStream, CompressionMode.Decompress))
            {
                using (MemoryStream memory = new MemoryStream(MaxStorageQueuePayloadSizeInBytes * 2))
                {
                    byte[] buffer = this.sharedBufferManager.TakeBuffer(DefaultBufferSize);
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
                        this.sharedBufferManager.ReturnBuffer(buffer);
                    }

                    return new ArraySegment<byte>(memory.GetBuffer(), 0, (int)memory.Length).Array;
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
        internal async Task UploadToBlobAsync(byte[] data, int dataByteCount, string blobName)
        {
            await this.cloudBlobContainer.CreateIfNotExistsAsync();
            CloudBlockBlob cloudBlockBlob = this.cloudBlobContainer.GetBlockBlobReference(blobName);
            await cloudBlockBlob.UploadFromByteArrayAsync(data, 0, dataByteCount);
        }
    }
}
