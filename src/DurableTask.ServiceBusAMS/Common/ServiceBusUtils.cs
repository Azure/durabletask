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

namespace DurableTask.ServiceBus.Common.Abstraction
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Runtime.ExceptionServices;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Core.Serialization;
    using Azure.Messaging.ServiceBus;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.History;
    using DurableTask.Core.Settings;
    using DurableTask.Core.Tracing;
    using DurableTask.Core.Tracking;
    using DurableTask.ServiceBus.Settings;
    using Newtonsoft.Json;


    internal static class ServiceBusUtils
    {
        internal static readonly TimeSpan TokenTimeToLive = TimeSpan.FromDays(30);

        public static Task<ServiceBusMessage> GetBrokeredMessageFromObjectAsync(object serializableObject, CompressionSettings compressionSettings)
        {
            return GetBrokeredMessageFromObjectAsync(serializableObject, compressionSettings, new ServiceBusMessageSettings(), null, null, null, DateTimeUtils.MinDateTime);
        }

        public static async Task<ServiceBusMessage> GetBrokeredMessageFromObjectAsync(
            object serializableObject,
            CompressionSettings compressionSettings,
            ServiceBusMessageSettings messageSettings,
            OrchestrationInstance instance,
            string messageType,
            IOrchestrationServiceBlobStore orchestrationServiceBlobStore,
            DateTime messageFireTime)
        {
            if (serializableObject == null)
            {
                throw new ArgumentNullException(nameof(serializableObject));
            }

            if (compressionSettings.Style == CompressionStyle.Legacy)
            {
                var serializer = new JsonObjectSerializer();
                var binaryData = await serializer.SerializeAsync(serializableObject);
                return new ServiceBusMessage(binaryData);
            }

            if (messageSettings == null)
            {
                messageSettings = new ServiceBusMessageSettings();
            }

            var disposeStream = true;
            var rawStream = new MemoryStream();

            Utils.WriteObjectToStream(rawStream, serializableObject);

            try
            {
                ServiceBusMessage brokeredMessage;

                if (compressionSettings.Style == CompressionStyle.Always ||
                   (compressionSettings.Style == CompressionStyle.Threshold &&
                     rawStream.Length > compressionSettings.ThresholdInBytes))
                {
                    Stream compressedStream = Utils.GetCompressedStream(rawStream);

                    if (compressedStream.Length < messageSettings.MessageOverflowThresholdInBytes)
                    {
                        brokeredMessage = GenerateBrokeredMessageWithCompressionTypeProperty(compressedStream, FrameworkConstants.CompressionTypeGzipPropertyValue);
                        long rawLen = rawStream.Length;
                        TraceHelper.TraceInstance(
                            TraceEventType.Information,
                            "GetBrokeredMessageFromObject-CompressionStats",
                            instance,
                            () =>
                                "Compression stats for " + (messageType ?? string.Empty) + " : " +
                                brokeredMessage?.MessageId +
                                ", uncompressed " + rawLen + " -> compressed " + compressedStream.Length);
                    }
                    else
                    {
                        brokeredMessage = await GenerateBrokeredMessageWithBlobKeyPropertyAsync(compressedStream, orchestrationServiceBlobStore, instance, messageSettings, messageFireTime, FrameworkConstants.CompressionTypeGzipPropertyValue);
                    }
                }
                else
                {
                    if (rawStream.Length < messageSettings.MessageOverflowThresholdInBytes)
                    {
                        brokeredMessage = GenerateBrokeredMessageWithCompressionTypeProperty(rawStream, FrameworkConstants.CompressionTypeNonePropertyValue);
                        disposeStream = false;
                    }
                    else
                    {
                        brokeredMessage = await GenerateBrokeredMessageWithBlobKeyPropertyAsync(rawStream, orchestrationServiceBlobStore, instance, messageSettings, messageFireTime, FrameworkConstants.CompressionTypeNonePropertyValue);
                    }
                }

                brokeredMessage.SessionId = instance?.InstanceId;
                // TODO : Test more if this helps, initial tests shows not change in performance
                // brokeredMessage.ViaPartitionKey = instance?.InstanceId;

                return brokeredMessage;
            }
            finally
            {
                if (disposeStream)
                {
                    rawStream.Dispose();
                }
            }
        }

        static ServiceBusMessage GenerateBrokeredMessageWithCompressionTypeProperty(Stream stream, string compressionType)
        {
            ServiceBusMessage brokeredMessage;
            using (var ms = new MemoryStream())
            {
                stream.CopyTo(ms);
                brokeredMessage = new ServiceBusMessage(ms.ToArray());
            }

            brokeredMessage.ApplicationProperties[FrameworkConstants.CompressionTypePropertyName] = compressionType;

            return brokeredMessage;
        }

        static async Task<ServiceBusMessage> GenerateBrokeredMessageWithBlobKeyPropertyAsync(
            Stream stream,
            IOrchestrationServiceBlobStore orchestrationServiceBlobStore,
            OrchestrationInstance instance,
            ServiceBusMessageSettings messageSettings,
            DateTime messageFireTime,
            string compressionType)
        {
            if (stream.Length > messageSettings.MessageMaxSizeInBytes)
            {
                throw new ArgumentException(
                    $"The serialized message size {stream.Length} is larger than the supported external storage blob size {messageSettings.MessageMaxSizeInBytes}.",
                    nameof(stream));
            }

            if (orchestrationServiceBlobStore == null)
            {
                throw new ArgumentException(
                    "Please provide an implementation of IOrchestrationServiceBlobStore for external storage.",
                    nameof(orchestrationServiceBlobStore));
            }

            // save the compressed stream using external storage when it is larger
            // than the supported message size limit.
            // the stream is stored using the generated key, which is saved in the message property.
            string blobKey = orchestrationServiceBlobStore.BuildMessageBlobKey(instance, messageFireTime);

            TraceHelper.TraceInstance(
                TraceEventType.Information,
                "GenerateBrokeredMessageWithBlobKeyProperty-SaveToBlob",
                instance,
                () => $"Saving the message stream in blob storage using key {blobKey}.");
            await orchestrationServiceBlobStore.SaveStreamAsync(blobKey, stream);

            var brokeredMessage = new ServiceBusMessage();
            brokeredMessage.ApplicationProperties[ServiceBusConstants.MessageBlobKey] = blobKey;
            brokeredMessage.ApplicationProperties[FrameworkConstants.CompressionTypePropertyName] = compressionType;

            return brokeredMessage;
        }

        public static async Task<T> GetObjectFromBrokeredMessageAsync<T>(ServiceBusReceivedMessage message, IOrchestrationServiceBlobStore orchestrationServiceBlobStore)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            T deserializedObject;

            string compressionType = string.Empty;

            if (message.ApplicationProperties.TryGetValue(FrameworkConstants.CompressionTypePropertyName, out object compressionTypeObj))
            {
                compressionType = (string)compressionTypeObj;
            }

            if (string.IsNullOrWhiteSpace(compressionType))
            {
                // no compression, legacy style
                return message.Body.ToObject<T>(new JsonObjectSerializer());
            }
            else if (string.Equals(compressionType, FrameworkConstants.CompressionTypeGzipPropertyValue,
                StringComparison.OrdinalIgnoreCase))
            {
                using (Stream compressedStream = await LoadMessageStreamAsync(message, orchestrationServiceBlobStore))
                {
                    if (!Utils.IsGzipStream(compressedStream))
                    {
                        throw new ArgumentException(
                            $"message specifies a CompressionType of {compressionType} but content is not compressed",
                            nameof(message));
                    }

                    using (Stream objectStream = await Utils.GetDecompressedStreamAsync(compressedStream))
                    {
                        deserializedObject = SafeReadFromStream<T>(objectStream);
                    }
                }
            }
            else if (string.Equals(compressionType, FrameworkConstants.CompressionTypeNonePropertyValue,
                StringComparison.OrdinalIgnoreCase))
            {
                using (Stream rawStream = await LoadMessageStreamAsync(message, orchestrationServiceBlobStore))
                {
                    deserializedObject = SafeReadFromStream<T>(rawStream);
                }
            }
            else
            {
                throw new ArgumentException(
                    $"message specifies an invalid CompressionType: {compressionType}",
                    nameof(message));
            }

            return deserializedObject;
        }

        static T SafeReadFromStream<T>(Stream objectStream)
        {
            byte[] serializedBytes = Utils.ReadBytesFromStream(objectStream);
            try
            {
                return Utils.ReadObjectFromByteArray<T>(serializedBytes);
            }
            catch (JsonSerializationException jex) when (typeof(T) == typeof(TaskMessage))
            {
                //If deserialization to TaskMessage fails attempt to deserialize to the now deprecated StateMessage type
#pragma warning disable 618
                ExceptionDispatchInfo originalException = ExceptionDispatchInfo.Capture(jex);
                StateMessage stateMessage = null;
                try
                {
                    stateMessage = Utils.ReadObjectFromByteArray<StateMessage>(serializedBytes);
                }
                catch (JsonSerializationException)
                {
                    originalException.Throw();
                }

                return (dynamic)new TaskMessage
                {
                    Event = new HistoryStateEvent(-1, stateMessage.State),
                    OrchestrationInstance = stateMessage.State.OrchestrationInstance
                };
#pragma warning restore 618
            }
        }

        static Task<Stream> LoadMessageStreamAsync(ServiceBusReceivedMessage message, IOrchestrationServiceBlobStore orchestrationServiceBlobStore)
        {
            string blobKey = string.Empty;

            if (message.ApplicationProperties.TryGetValue(ServiceBusConstants.MessageBlobKey, out object blobKeyObj))
            {
                blobKey = (string)blobKeyObj;
            }

            if (string.IsNullOrWhiteSpace(blobKey))
            {
                // load the stream from the message directly if the blob key property is not set,
                // i.e., it is not stored externally
                return Task.Run(() => message.Body.ToStream());
            }

            // if the blob key is set in the message property,
            // load the stream message from the service bus message store.
            if (orchestrationServiceBlobStore == null)
            {
                throw new ArgumentException($"Failed to load compressed message from external storage with key: {blobKey}. Please provide an implementation of IServiceBusMessageStore for external storage.", nameof(orchestrationServiceBlobStore));
            }

            return orchestrationServiceBlobStore.LoadStreamAsync(blobKey);
        }

        public static void CheckAndLogDeliveryCount(string sessionId, IEnumerable<ServiceBusReceivedMessage> messages, int maxDeliveryCount)
        {
            foreach (ServiceBusReceivedMessage message in messages)
            {
                CheckAndLogDeliveryCount(sessionId, message, maxDeliveryCount);
            }
        }

        public static void CheckAndLogDeliveryCount(IEnumerable<ServiceBusReceivedMessage> messages, int maxDeliveryCount)
        {
            foreach (ServiceBusReceivedMessage message in messages)
            {
                CheckAndLogDeliveryCount(message, maxDeliveryCount);
            }
        }

        public static void CheckAndLogDeliveryCount(ServiceBusReceivedMessage message, int maxDeliveryCount)
        {
            CheckAndLogDeliveryCount(null, message, maxDeliveryCount);
        }

        public static void CheckAndLogDeliveryCount(string sessionId, ServiceBusReceivedMessage message, int maxDeliveryCount)
        {
            if (message.DeliveryCount >= maxDeliveryCount - 2)
            {
                if (!string.IsNullOrWhiteSpace(sessionId))
                {
                    TraceHelper.TraceSession(
                        TraceEventType.Critical,
                        "MaxDeliveryCountApproaching-Session",
                        sessionId,
                        "Delivery count for message with id {0} is {1}. Message will be deadlettered if processing continues to fail.",
                        message.MessageId,
                        message.DeliveryCount);
                }
                else
                {
                    TraceHelper.Trace(
                        TraceEventType.Critical,
                        "MaxDeliveryCountApproaching",
                        "Delivery count for message with id {0} is {1}. Message will be deadlettered if processing continues to fail.",
                        message.MessageId, 
                        message.DeliveryCount);
                }
            }
        }
    }
}
