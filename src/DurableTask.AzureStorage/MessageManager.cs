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

namespace DurableTask.AzureStorage
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.IO.Compression;
    using System.Reflection;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.Core;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Queue;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Serialization;

    /// <summary>
    /// The message manager for messages from MessageData, and DynamicTableEntities
    /// </summary>
    class MessageManager
    {
        const int MaxStorageQueuePayloadSizeInBytes = 60 * 1024; // 60KB
        const int DefaultBufferSize = 64 * 2014; // 64KB

        const string LargeMessageBlobNameSeparator = "/";
        const string blobExtension = ".json.gz";

        readonly string blobContainerName;
        readonly CloudBlobContainer cloudBlobContainer;
        readonly JsonSerializerSettings taskMessageSerializerSettings;

        bool containerInitialized;

        /// <summary>
        /// The message manager.
        /// </summary>
        public MessageManager(CloudBlobClient cloudBlobClient, string blobContainerName)
        {
            this.blobContainerName = blobContainerName;
            this.cloudBlobContainer = cloudBlobClient.GetContainerReference(blobContainerName);
            this.taskMessageSerializerSettings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Objects,
#if NETSTANDARD2_0
                SerializationBinder = new TypeNameSerializationBinder(),
#else
                Binder = new TypeNameSerializationBinder(),
#endif
            };
        }

        public async Task<bool> EnsureContainerAsync()
        {
            bool created = false;

            if (!this.containerInitialized)
            {
                created = await this.cloudBlobContainer.CreateIfNotExistsAsync();
                this.containerInitialized = true;
            }

            return created;
        }

        public async Task<bool> DeleteContainerAsync()
        {
            bool deleted = await this.cloudBlobContainer.DeleteIfExistsAsync();
            this.containerInitialized = false;
            return deleted;
        }

        /// <summary>
        /// Serializes the MessageData object
        /// </summary>
        /// <param name="messageData">Instance of <see cref="MessageData"/></param>
        /// <returns>JSON for the <see cref="MessageData"/> object</returns>
        public async Task<string> SerializeMessageDataAsync(MessageData messageData)
        {
            string rawContent = JsonConvert.SerializeObject(messageData, this.taskMessageSerializerSettings);
            messageData.TotalMessageSizeBytes = Encoding.Unicode.GetByteCount(rawContent);
            MessageFormatFlags messageFormat = this.GetMessageFormatFlags(messageData);

            if (messageFormat != MessageFormatFlags.InlineJson)
            {
                // Get Compressed bytes and upload the full message to blob storage.
                byte[] messageBytes = Encoding.UTF8.GetBytes(rawContent);
                string blobName = this.GetNewLargeMessageBlobName(messageData);
                messageData.CompressedBlobName = blobName;
                await this.CompressAndUploadAsBytesAsync(messageBytes, blobName);

                // Create a "wrapper" message which has the blob name but not a task message.
                var wrapperMessageData = new MessageData { CompressedBlobName = blobName };
                return JsonConvert.SerializeObject(wrapperMessageData, this.taskMessageSerializerSettings);
            }

            return JsonConvert.SerializeObject(messageData, this.taskMessageSerializerSettings);
        }

        /// <summary>
        /// If the "message" of an orchestration state is actually a URI retrieves actual message from blob storage.
        /// Otherwise returns the message as is.
        /// </summary>
        /// <param name="message">The message to be fetched if it is a url.</param>
        /// <returns>Actual string representation of message.</returns>
        public async Task<string> FetchLargeMessageIfNecessary(string message)
        {
            if (Uri.IsWellFormedUriString(message, UriKind.Absolute))
            {
                return await this.DownloadAndDecompressAsBytesAsync(new Uri(message));
            }
            else
            {
                return message;
            }
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

        internal Task CompressAndUploadAsBytesAsync(byte[] payloadBuffer, string blobName)
        {
            ArraySegment<byte> compressedSegment = this.Compress(payloadBuffer);
            return this.UploadToBlobAsync(compressedSegment.Array, compressedSegment.Count, blobName);
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

        internal async Task<string> DownloadAndDecompressAsBytesAsync(string blobName)
        {
            await this.EnsureContainerAsync();

            CloudBlockBlob cloudBlockBlob = this.cloudBlobContainer.GetBlockBlobReference(blobName);
            return await DownloadAndDecompressAsBytesAsync(cloudBlockBlob);
        }

        internal Task<string> DownloadAndDecompressAsBytesAsync(Uri blobUri)
        {
            CloudBlockBlob cloudBlockBlob = new CloudBlockBlob(blobUri, this.cloudBlobContainer.ServiceClient.Credentials);
            return DownloadAndDecompressAsBytesAsync(cloudBlockBlob);
        }

        private async Task<string> DownloadAndDecompressAsBytesAsync(CloudBlockBlob cloudBlockBlob)
        {
            Stream downloadBlobAsStream = await cloudBlockBlob.OpenReadAsync();
            ArraySegment<byte> decompressedSegment = this.Decompress(downloadBlobAsStream);
            return Encoding.UTF8.GetString(decompressedSegment.Array, 0, decompressedSegment.Count);
        }

        internal string GetBlobUrl(string blobName)
        {
            return this.cloudBlobContainer.GetBlockBlobReference(blobName).Uri.AbsoluteUri;
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
        internal async Task UploadToBlobAsync(byte[] data, int dataByteCount, string blobName)
        {
            await this.EnsureContainerAsync();

            CloudBlockBlob cloudBlockBlob = this.cloudBlobContainer.GetBlockBlobReference(blobName);
            await cloudBlockBlob.UploadFromByteArrayAsync(data, 0, dataByteCount);
        }

        internal string GetNewLargeMessageBlobName(MessageData message)
        {
            string instanceId = message.TaskMessage.OrchestrationInstance.InstanceId;
            string eventType = message.TaskMessage.Event.EventType.ToString();
            string sequenceNumber = message.SequenceNumber.ToString("X16");

            return $"{instanceId}/message-{sequenceNumber}-{eventType}.json.gz".ToLowerInvariant();
        }

        internal async Task DeleteLargeMessageBlobs(string instanceId, AzureStorageOrchestrationServiceStats stats)
        {
            var blobForDeletionTaskList = new List<Task>();
            if (!await this.cloudBlobContainer.ExistsAsync())
            {
                return;
            }

            CloudBlobDirectory instanceDirectory = this.cloudBlobContainer.GetDirectoryReference(instanceId);
            BlobContinuationToken blobContinuationToken = null;
            while (true)
            {
                BlobResultSegment segment = await instanceDirectory.ListBlobsSegmentedAsync(blobContinuationToken);
                stats.StorageRequests.Increment();
                foreach (IListBlobItem blobListItem in segment.Results)
                {
                    var cloudBlockBlob = blobListItem as CloudBlockBlob;
                    CloudBlockBlob blob = this.cloudBlobContainer.GetBlockBlobReference(cloudBlockBlob?.Name);
                    blobForDeletionTaskList.Add(blob.DeleteIfExistsAsync());
                }

                await Task.WhenAll(blobForDeletionTaskList);

                stats.StorageRequests.Increment(blobForDeletionTaskList.Count);
                if (blobContinuationToken == null)
                {
                    break;
                }
            }
        }
    }

#if NETSTANDARD2_0
    class TypeNameSerializationBinder : ISerializationBinder
    {
        public void BindToName(Type serializedType, out string assemblyName, out string typeName)
        {
            TypeNameSerializationHelper.BindToName(serializedType, out assemblyName, out typeName);
        }

        public Type BindToType(string assemblyName, string typeName)
        {
            return TypeNameSerializationHelper.BindToType(assemblyName, typeName);
        }
    }
#else
    class TypeNameSerializationBinder : SerializationBinder
    {
        public override void BindToName(Type serializedType, out string assemblyName, out string typeName)
        {
            TypeNameSerializationHelper.BindToName(serializedType, out assemblyName, out typeName);
        }

        public override Type BindToType(string assemblyName, string typeName)
        {
            return TypeNameSerializationHelper.BindToType(assemblyName, typeName);
        }
    }
#endif
    static class TypeNameSerializationHelper
    {
        static readonly Assembly DurableTaskCore = typeof(DurableTask.Core.TaskMessage).Assembly;
        static readonly Assembly DurableTaskAzureStorage = typeof(AzureStorageOrchestrationService).Assembly;

        public static void BindToName(Type serializedType, out string assemblyName, out string typeName)
        {
            assemblyName = null;
            typeName = serializedType.FullName;
        }

        public static Type BindToType(string assemblyName, string typeName)
        {
            if (typeName.StartsWith("DurableTask.Core"))
            {
                return DurableTaskCore.GetType(typeName, throwOnError: true);
            }
            else if (typeName.StartsWith("DurableTask.AzureStorage"))
            {
                return DurableTaskAzureStorage.GetType(typeName, throwOnError: true);
            }

            return Type.GetType(typeName, throwOnError: true);
        }
    }
}
