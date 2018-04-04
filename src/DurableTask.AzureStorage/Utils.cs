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
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage.Queue;
    using Newtonsoft.Json;

    static class Utils
    {
        const int MaxStorageQueuePayloadSizeInBytes = 60 * 1024; // 60KB
        const int DefaultBufferSize = 4096;

        public static readonly Task CompletedTask = Task.FromResult(0);

        static readonly JsonSerializerSettings TaskMessageSerializerSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.Objects
        };

        public static string SerializeMessageData(string storageConnectionString, MessageData messageData)
        {
            string rawContent = JsonConvert.SerializeObject(messageData, TaskMessageSerializerSettings);
            messageData.TotalMessageSizeBytes = Encoding.UTF8.GetByteCount(rawContent);
            MessageFormatFlags messageFormat = GetMessageFormatFlags(messageData);

            if (messageFormat != MessageFormatFlags.InlineJson)
            {
                byte[] messageBytes = Encoding.UTF8.GetBytes(rawContent);

                // Get Compressed bytes
                string blobName = messageData.ActivityId.ToString();
                CompressAndUploadAsBytes(storageConnectionString, messageBytes, blobName);

                MessageData wrapperMessageData = new MessageData
                {
                    CompressedBlobName = blobName
                };

                return JsonConvert.SerializeObject(wrapperMessageData, TaskMessageSerializerSettings);
            }

            return JsonConvert.SerializeObject(messageData, TaskMessageSerializerSettings);
        }

        public static MessageData DeserializeQueueMessage(string storageConnectionString, CloudQueueMessage queueMessage, string queueName)
        {
            MessageData envelope = JsonConvert.DeserializeObject<MessageData>(
                queueMessage.AsString,
                TaskMessageSerializerSettings);

            if (!string.IsNullOrEmpty(envelope.CompressedBlobName))
            {
                string decompressedMessage = DownloadAndDecompressAsBytes(storageConnectionString, envelope.CompressedBlobName);
                envelope = JsonConvert.DeserializeObject<MessageData>(
                    decompressedMessage,
                    TaskMessageSerializerSettings);
            }

            envelope.OriginalQueueMessage = queueMessage;
            envelope.TotalMessageSizeBytes = Encoding.UTF8.GetByteCount(queueMessage.AsString);
            envelope.QueueName = queueName;
            return envelope;
        }

        public static async Task ParallelForEachAsync<TSource>(
            this IEnumerable<TSource> enumerable,
            Func<TSource, Task> createTask)
        {
            var tasks = new List<Task>();
            foreach (TSource entry in enumerable)
            {
                tasks.Add(createTask(entry));
            }

            await Task.WhenAll(tasks.ToArray());
        }

        public static bool ExceedsMaxTableEntitySize(string data)
        {
            if (!string.IsNullOrEmpty(data) && Encoding.UTF8.GetByteCount(data) > MaxStorageQueuePayloadSizeInBytes)
            {
                return true;
            }

            return false;
        }

        internal static void CompressAndUploadAsBytes(string storageConnectionString, byte[] payloadBuffer, string activationId)
        {
            byte[] compressedBytes = Compress(payloadBuffer);
            if (!StorageUtil.Instance.IsInitialized())
            {
                StorageUtil.Instance.Initialize(storageConnectionString);
            }

            StorageUtil.Instance.UploadToBlob(compressedBytes, compressedBytes.Length, activationId);
        }

        internal static byte[] Compress(byte[] payloadBuffer)
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

        internal static string DownloadAndDecompressAsBytes(string storageConnectionString, string blobName)
        {
            if (!StorageUtil.Instance.IsInitialized())
            {
                StorageUtil.Instance.Initialize(storageConnectionString);
            }

            byte[] downloadBlobAsAsBytes = StorageUtil.Instance.DownloadBlob(blobName);
            byte[] decompressedBytes = Decompress(downloadBlobAsAsBytes);
            return Encoding.UTF8.GetString(decompressedBytes);
        }

        internal static byte[] Decompress(byte[] gzip)
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

        internal static MessageFormatFlags GetMessageFormatFlags(MessageData messageData)
        {
            MessageFormatFlags messageFormatFlags = MessageFormatFlags.InlineJson;

            if (messageData.TotalMessageSizeBytes > MaxStorageQueuePayloadSizeInBytes)
            {
                messageFormatFlags = MessageFormatFlags.StorageBlob;
            }

            return messageFormatFlags;
        }
    }
}
