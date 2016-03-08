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

namespace DurableTask.Common
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.IO.Compression;
    using System.Runtime.ExceptionServices;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;
    using Newtonsoft.Json;
    using Tracing;

    internal static class ServiceBusUtils
    {
        public static BrokeredMessage GetBrokeredMessageFromObject(object serializableObject, CompressionSettings compressionSettings)
        {
            return GetBrokeredMessageFromObject(serializableObject, compressionSettings, null, null);
        }

        public static BrokeredMessage GetBrokeredMessageFromObject(
            object serializableObject,
            CompressionSettings compressionSettings,
            OrchestrationInstance instance, 
            string messageType)
        {
            if (serializableObject == null)
            {
                throw new ArgumentNullException(nameof(serializableObject));
            }

            if (compressionSettings.Style == CompressionStyle.Legacy)
            {
                return new BrokeredMessage(serializableObject);
            }

            bool disposeStream = true;
            var rawStream = new MemoryStream();

            Utils.WriteObjectToStream(rawStream, serializableObject);

            try
            {
                BrokeredMessage brokeredMessage = null;

                if (compressionSettings.Style == CompressionStyle.Always ||
                    (compressionSettings.Style == CompressionStyle.Threshold && 
                     rawStream.Length > compressionSettings.ThresholdInBytes))
                {
                    Stream compressedStream = Utils.GetCompressedStream(rawStream);

                    brokeredMessage = new BrokeredMessage(compressedStream, true);
                    brokeredMessage.Properties[FrameworkConstants.CompressionTypePropertyName] =
                        FrameworkConstants.CompressionTypeGzipPropertyValue;

                    var rawLen = rawStream.Length;
                    TraceHelper.TraceInstance(TraceEventType.Information, instance,
                        () =>
                            "Compression stats for " + (messageType ?? string.Empty) + " : " + brokeredMessage.MessageId +
                            ", uncompressed " + rawLen + " -> compressed " + compressedStream.Length);
                }
                else
                {
                    brokeredMessage = new BrokeredMessage(rawStream, true);
                    disposeStream = false;
                    brokeredMessage.Properties[FrameworkConstants.CompressionTypePropertyName] =
                        FrameworkConstants.CompressionTypeNonePropertyValue;
                }

                brokeredMessage.SessionId = instance?.InstanceId;

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

        public static async Task<T> GetObjectFromBrokeredMessageAsync<T>(BrokeredMessage message)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }

            T deserializedObject;

            object compressionTypeObj = null;
            string compressionType = string.Empty;

            if (message.Properties.TryGetValue(FrameworkConstants.CompressionTypePropertyName, out compressionTypeObj))
            {
                compressionType = (string)compressionTypeObj;
            }

            if (string.IsNullOrEmpty(compressionType))
            {
                // no compression, legacy style
                deserializedObject = message.GetBody<T>();
            }
            else if (string.Equals(compressionType, FrameworkConstants.CompressionTypeGzipPropertyValue,
                StringComparison.OrdinalIgnoreCase))
            {
                using (var compressedStream = message.GetBody<Stream>())
                {
                    if (!Utils.IsGzipStream(compressedStream))
                    {
                        throw new ArgumentException(
                            "message specifies a CompressionType of " + compressionType +
                            " but content is not compressed",
                            "message");
                    }

                    using (Stream objectStream = await Utils.GetDecompressedStreamAsync(compressedStream))
                    {
                        deserializedObject = Utils.ReadObjectFromStream<T>(objectStream);
                    }
                }
            }
            else if (string.Equals(compressionType, FrameworkConstants.CompressionTypeNonePropertyValue,
                StringComparison.OrdinalIgnoreCase))
            {
                using (var rawStream = message.GetBody<Stream>())
                {
                    deserializedObject = Utils.ReadObjectFromStream<T>(rawStream);
                }
            }
            else
            {
                throw new ArgumentException("message specifies an invalid CompressionType: " + compressionType,
                    "message");
            }

            return deserializedObject;
        }

        public static void CheckAndLogDeliveryCount(BrokeredMessage message, int maxDeliverycount)
        {
            CheckAndLogDeliveryCount(null, message, maxDeliverycount);
        }

        public static void CheckAndLogDeliveryCount(string sessionId, BrokeredMessage message, int maxDeliveryCount)
        {
            if (message.DeliveryCount >= maxDeliveryCount - 2)
            {
                if (!string.IsNullOrEmpty(sessionId))
                {
                    TraceHelper.TraceSession(TraceEventType.Critical, sessionId,
                        "Delivery count for message with id {0} is {1}. Message will be deadlettered if processing continues to fail.",
                        message.MessageId, message.DeliveryCount);
                }
                else
                {
                    TraceHelper.Trace(TraceEventType.Critical,
                        "Delivery count for message with id {0} is {1}. Message will be deadlettered if processing continues to fail.",
                        message.MessageId, message.DeliveryCount);
                }
            }
        }

        public static MessagingFactory CreateMessagingFactory(string connectionString)
        {
            MessagingFactory factory = MessagingFactory.CreateFromConnectionString(connectionString);
            factory.RetryPolicy = RetryPolicy.Default;
            return factory;
        }
    }
}
