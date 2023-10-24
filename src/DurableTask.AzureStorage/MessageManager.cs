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
    using System.Linq;
    using System.Reflection;
#if !NETSTANDARD2_0
    using System.Runtime.Serialization;
#endif
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Storage.Blobs.Models;
    using Azure.Storage.Queues.Models;
    using DurableTask.AzureStorage.Storage;
    using Newtonsoft.Json;
#if NETSTANDARD2_0
    using Newtonsoft.Json.Serialization;
#endif

    /// <summary>
    /// The message manager for messages from MessageData, and DynamicTableEntities
    /// </summary>
    class MessageManager
    {
        // Use 45 KB, as the Storage SDK will base64 encode the message,
        // increasing the size by a factor of 4/3.
        const int MaxStorageQueuePayloadSizeInBytes = 45 * 1024; // 45KB
        const int DefaultBufferSize = 64 * 2014; // 64KB

        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly AzureStorageClient azureStorageClient;
        readonly BlobContainer blobContainer;
        readonly JsonSerializerSettings taskMessageSerializerSettings;
        readonly JsonSerializer serializer;

        bool containerInitialized;

        public MessageManager(
            AzureStorageOrchestrationServiceSettings settings,
            AzureStorageClient azureStorageClient,
            string blobContainerName)
        {
            this.settings = settings;
            this.azureStorageClient = azureStorageClient;
            this.blobContainer = this.azureStorageClient.GetBlobContainerReference(blobContainerName);
            this.taskMessageSerializerSettings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Objects,
#if NETSTANDARD2_0
                SerializationBinder = new TypeNameSerializationBinder(settings.CustomMessageTypeBinder),
#else
                Binder = new TypeNameSerializationBinder(settings.CustomMessageTypeBinder),
#endif
            };
            this.serializer = JsonSerializer.Create(taskMessageSerializerSettings);

            if (this.settings.UseDataContractSerialization)
            {
                this.taskMessageSerializerSettings.Converters.Add(new DataContractJsonConverter());
            }
        }

        public async Task<bool> EnsureContainerAsync(CancellationToken cancellationToken = default)
        {
            bool created = false;

            if (!this.containerInitialized)
            {
                created = await this.blobContainer.CreateIfNotExistsAsync(cancellationToken);
                this.containerInitialized = true;
            }

            return created;
        }

        public async Task<bool> DeleteContainerAsync(CancellationToken cancellationToken = default)
        {
            bool deleted = await this.blobContainer.DeleteIfExistsAsync(cancellationToken: cancellationToken);
            this.containerInitialized = false;
            return deleted;
        }

        public async Task<string> SerializeMessageDataAsync(MessageData messageData, CancellationToken cancellationToken = default)
        {
            string rawContent = Utils.SerializeToJson(serializer, messageData);
            messageData.TotalMessageSizeBytes = Encoding.UTF8.GetByteCount(rawContent);
            MessageFormatFlags messageFormat = this.GetMessageFormatFlags(messageData);

            if (messageFormat != MessageFormatFlags.InlineJson)
            {
                // Get Compressed bytes and upload the full message to blob storage.
                byte[] messageBytes = Encoding.UTF8.GetBytes(rawContent);
                string blobName = this.GetNewLargeMessageBlobName(messageData);
                messageData.CompressedBlobName = blobName;
                await this.CompressAndUploadAsBytesAsync(messageBytes, blobName, cancellationToken);

                // Create a "wrapper" message which has the blob name but not a task message.
                var wrapperMessageData = new MessageData { CompressedBlobName = blobName };
                return Utils.SerializeToJson(serializer, wrapperMessageData);
            }

            return Utils.SerializeToJson(serializer, messageData);
        }

        /// <summary>
        /// If the "message" of an orchestration state is actually a URI retrieves actual message from blob storage.
        /// Otherwise returns the message as is.
        /// </summary>
        /// <param name="message">The message to be fetched if it is a url.</param>
        /// <param name="cancellationToken">A token used for canceling the operation.</param>
        /// <returns>Actual string representation of message.</returns>
        public async Task<string> FetchLargeMessageIfNecessary(string message, CancellationToken cancellationToken = default)
        {
            if (TryGetLargeMessageReference(message, out Uri blobUrl))
            {
                return await this.DownloadAndDecompressAsBytesAsync(blobUrl, cancellationToken);
            }
            else
            {
                return message;
            }
        }

        internal static bool TryGetLargeMessageReference(string messagePayload, out Uri blobUrl)
        {
            if (Uri.IsWellFormedUriString(messagePayload, UriKind.Absolute))
            {
                return Uri.TryCreate(messagePayload, UriKind.Absolute, out blobUrl);
            }

            blobUrl = null;
            return false;
        }

        public async Task<MessageData> DeserializeQueueMessageAsync(QueueMessage queueMessage, string queueName, CancellationToken cancellationToken = default)
        {
            // TODO: Deserialize with Stream?
            byte[] body = queueMessage.Body.ToArray();
            MessageData envelope = this.DeserializeMessageData(Encoding.UTF8.GetString(body));

            if (!string.IsNullOrEmpty(envelope.CompressedBlobName))
            {
                string decompressedMessage = await this.DownloadAndDecompressAsBytesAsync(envelope.CompressedBlobName, cancellationToken);
                envelope = this.DeserializeMessageData(decompressedMessage);
                envelope.MessageFormat = MessageFormatFlags.StorageBlob;
                envelope.TotalMessageSizeBytes = Encoding.UTF8.GetByteCount(decompressedMessage);
            }
            else
            {
                envelope.TotalMessageSizeBytes = body.Length;
            }

            envelope.OriginalQueueMessage = queueMessage;
            envelope.QueueName = queueName;
            return envelope;
        }

        internal MessageData DeserializeMessageData(string json)
        {
            return Utils.DeserializeFromJson<MessageData>(this.serializer, json);
        }

        public async Task CompressAndUploadAsBytesAsync(byte[] payloadBuffer, string blobName, CancellationToken cancellationToken = default)
        {
            await this.EnsureContainerAsync(cancellationToken);

            Blob blob = this.blobContainer.GetBlobReference(blobName);
            using Stream blobStream = await blob.OpenWriteAsync(cancellationToken);
            using GZipStream compressedBlobStream = new GZipStream(blobStream, CompressionLevel.Optimal);
            using MemoryStream payloadStream = new MemoryStream(payloadBuffer);

            try
            {
                // Note: 81920 bytes or 80 KB is the default value used by CopyToAsync
                await payloadStream.CopyToAsync(compressedBlobStream, bufferSize: 81920, cancellationToken: cancellationToken);
                await compressedBlobStream.FlushAsync(cancellationToken);
                await blobStream.FlushAsync(cancellationToken);
            }
            catch (RequestFailedException rfe)
            {
                throw new DurableTaskStorageException(rfe);
            }
        }

        public async Task<string> DownloadAndDecompressAsBytesAsync(string blobName, CancellationToken cancellationToken = default)
        {
            await this.EnsureContainerAsync(cancellationToken);

            Blob blob = this.blobContainer.GetBlobReference(blobName);
            return await DownloadAndDecompressAsBytesAsync(blob, cancellationToken);
        }

        public Task<string> DownloadAndDecompressAsBytesAsync(Uri blobUri, CancellationToken cancellationToken = default)
        {
            Blob blob = this.azureStorageClient.GetBlobReference(blobUri);
            return DownloadAndDecompressAsBytesAsync(blob, cancellationToken);
        }

        public Task<bool> DeleteBlobAsync(string blobName, CancellationToken cancellationToken = default)
        {
            Blob blob = this.blobContainer.GetBlobReference(blobName);
            return blob.DeleteIfExistsAsync(cancellationToken);
        }

        private async Task<string> DownloadAndDecompressAsBytesAsync(Blob blob, CancellationToken cancellationToken = default)
        {
            using BlobDownloadStreamingResult result = await blob.DownloadStreamingAsync(cancellationToken);
            using GZipStream decompressedBlobStream = new GZipStream(result.Content, CompressionMode.Decompress);
            using StreamReader reader = new StreamReader(decompressedBlobStream, Encoding.UTF8);

            return await reader.ReadToEndAsync();
        }

        public string GetBlobUrl(string blobName)
        {
            return Uri.UnescapeDataString(this.blobContainer.GetBlobReference(blobName).Uri.AbsoluteUri);
        }

        public MessageFormatFlags GetMessageFormatFlags(MessageData messageData)
        {
            MessageFormatFlags messageFormatFlags = MessageFormatFlags.InlineJson;

            if (messageData.TotalMessageSizeBytes > MaxStorageQueuePayloadSizeInBytes)
            {
                messageFormatFlags = MessageFormatFlags.StorageBlob;
            }

            return messageFormatFlags;
        }

        public async Task UploadToBlobAsync(byte[] data, int dataByteCount, string blobName, CancellationToken cancellationToken = default)
        {
            await this.EnsureContainerAsync(cancellationToken);

            Blob blob = this.blobContainer.GetBlobReference(blobName);
            await blob.UploadFromByteArrayAsync(data, 0, dataByteCount, cancellationToken);
        }

        public string GetNewLargeMessageBlobName(MessageData message)
        {
            string instanceId = message.TaskMessage.OrchestrationInstance.InstanceId;
            string eventType = message.TaskMessage.Event.EventType.ToString();
            string activityId = message.ActivityId.ToString("N");

            return $"{instanceId}/message-{activityId}-{eventType}.json.gz";
        }

        public async Task<int> DeleteLargeMessageBlobs(string sanitizedInstanceId, CancellationToken cancellationToken = default)
        {
            int storageOperationCount = 1;
            if (await this.blobContainer.ExistsAsync(cancellationToken))
            {
                await foreach (Page<Blob> page in this.blobContainer.ListBlobsAsync(sanitizedInstanceId, cancellationToken).AsPages())
                {
                    storageOperationCount++;

                    await Task.WhenAll(page.Values.Select(b => b.DeleteIfExistsAsync(cancellationToken)));

                    storageOperationCount += page.Values.Count;
                }
            }

            return storageOperationCount;
        }
    }

#if NETSTANDARD2_0
    class TypeNameSerializationBinder : ISerializationBinder
    {
        readonly ICustomTypeBinder customBinder;
        public TypeNameSerializationBinder(ICustomTypeBinder customBinder) 
        {
            this.customBinder = customBinder;
        }

        public void BindToName(Type serializedType, out string assemblyName, out string typeName)
        {
            TypeNameSerializationHelper.BindToName(customBinder, serializedType, out assemblyName, out typeName);
        }

        public Type BindToType(string assemblyName, string typeName)
        {
            return TypeNameSerializationHelper.BindToType(customBinder, assemblyName, typeName);
        }
    }
#else
    class TypeNameSerializationBinder : SerializationBinder
    {
        readonly ICustomTypeBinder customBinder;
        public TypeNameSerializationBinder(ICustomTypeBinder customBinder)
        {
            this.customBinder = customBinder;
        }

        public override void BindToName(Type serializedType, out string assemblyName, out string typeName)
        {
            TypeNameSerializationHelper.BindToName(customBinder, serializedType, out assemblyName, out typeName);
        }

        public override Type BindToType(string assemblyName, string typeName)
        {
            return TypeNameSerializationHelper.BindToType(customBinder, assemblyName, typeName);
        }
    }
#endif
    static class TypeNameSerializationHelper
    {
        static readonly Assembly DurableTaskCore = typeof(DurableTask.Core.TaskMessage).Assembly;
        static readonly Assembly DurableTaskAzureStorage = typeof(AzureStorageOrchestrationService).Assembly;

        public static void BindToName(ICustomTypeBinder customBinder, Type serializedType, out string assemblyName, out string typeName)
        {
            if (customBinder != null)
            {
                customBinder.BindToName(serializedType, out assemblyName, out typeName);
            }
            else
            {
                assemblyName = null;
                typeName = serializedType.FullName;
            }
        }

        public static Type BindToType(ICustomTypeBinder customBinder, string assemblyName, string typeName)
        {
            if (typeName.StartsWith("DurableTask.Core"))
            {
                return DurableTaskCore.GetType(typeName, throwOnError: true);
            }
            else if (typeName.StartsWith("DurableTask.AzureStorage"))
            {
                return DurableTaskAzureStorage.GetType(typeName, throwOnError: true);
            }

            return customBinder?.BindToType(assemblyName, typeName) ?? Type.GetType(typeName, throwOnError: true);
        }
    }
}
