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

    internal static class Utils
    {
        const int FullGzipHeaderLength = 10;

        public static readonly DateTime DateTimeSafeMaxValue =
            DateTime.MaxValue.Subtract(TimeSpan.FromDays(1)).ToUniversalTime();

        static readonly byte[] GzipHeader = {0x1f, 0x8b};

        public static string Truncate(this string input, int maxLength)
        {
            if (!string.IsNullOrEmpty(input) && input.Length > maxLength)
            {
                return input.Substring(0, maxLength);
            }
            return input;
        }

        public static BrokeredMessage GetBrokeredMessageFromObject(object serializableObject,
            CompressionSettings compressionSettings)
        {
            return GetBrokeredMessageFromObject(serializableObject, compressionSettings, null, null);
        }

        public static BrokeredMessage GetBrokeredMessageFromObject(object serializableObject,
            CompressionSettings compressionSettings,
            OrchestrationInstance instance, string messageType)
        {
            if (serializableObject == null)
            {
                throw new ArgumentNullException("serializableObject");
            }

            if (compressionSettings.Style == CompressionStyle.Legacy)
            {
                return new BrokeredMessage(serializableObject);
            }

            bool disposeStream = true;
            var rawStream = new MemoryStream();
            WriteObjectToStream(rawStream, serializableObject);

            try
            {
                BrokeredMessage brokeredMessage = null;

                if (compressionSettings.Style == CompressionStyle.Always ||
                    (compressionSettings.Style == CompressionStyle.Threshold && rawStream.Length >
                     compressionSettings.ThresholdInBytes))
                {
                    Stream compressedStream = GetCompressedStream(rawStream);

                    brokeredMessage = new BrokeredMessage(compressedStream, true);
                    brokeredMessage.Properties[FrameworkConstants.CompressionTypePropertyName] =
                        FrameworkConstants.CompressionTypeGzipPropertyValue;

                    TraceHelper.TraceInstance(TraceEventType.Information, instance,
                        () =>
                            "Compression stats for " + (messageType ?? string.Empty) + " : " + brokeredMessage.MessageId +
                            ", uncompressed " + rawStream.Length + " -> compressed " + compressedStream.Length);
                }
                else
                {
                    brokeredMessage = new BrokeredMessage(rawStream, true);
                    disposeStream = false;
                    brokeredMessage.Properties[FrameworkConstants.CompressionTypePropertyName] =
                        FrameworkConstants.CompressionTypeNonePropertyValue;
                }

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
                throw new ArgumentNullException("message");
            }

            T deserializedObject;

            object compressionTypeObj = null;
            string compressionType = string.Empty;

            if (message.Properties.TryGetValue(FrameworkConstants.CompressionTypePropertyName, out compressionTypeObj))
            {
                compressionType = (string) compressionTypeObj;
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
                    if (!IsGzipStream(compressedStream))
                    {
                        throw new ArgumentException(
                            "message specifies a CompressionType of " + compressionType +
                            " but content is not compressed",
                            "message");
                    }

                    using (Stream objectStream = await GetDecompressedStreamAsync(compressedStream))
                    {
                        deserializedObject = ReadObjectFromStream<T>(objectStream);
                    }
                }
            }
            else if (string.Equals(compressionType, FrameworkConstants.CompressionTypeNonePropertyValue,
                StringComparison.OrdinalIgnoreCase))
            {
                using (var rawStream = message.GetBody<Stream>())
                {
                    deserializedObject = ReadObjectFromStream<T>(rawStream);
                }
            }
            else
            {
                throw new ArgumentException("message specifies an invalid CompressionType: " + compressionType,
                    "message");
            }

            return deserializedObject;
        }

        static void WriteObjectToStream(Stream objectStream, object obj)
        {
            if (objectStream == null || !objectStream.CanWrite || !objectStream.CanSeek)
            {
                throw new ArgumentException("stream is not seekable or writable", "objectStream");
            }

            byte[] serializedBytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(obj,
                new JsonSerializerSettings {TypeNameHandling = TypeNameHandling.All}));

            objectStream.Write(serializedBytes, 0, serializedBytes.Length);
            objectStream.Position = 0;
        }

        static T ReadObjectFromStream<T>(Stream objectStream)
        {
            if (objectStream == null || !objectStream.CanRead || !objectStream.CanSeek)
            {
                throw new ArgumentException("stream is not seekable or readable", "objectStream");
            }

            objectStream.Position = 0;

            var serializedBytes = new byte[objectStream.Length];
            objectStream.Read(serializedBytes, 0, serializedBytes.Length);
            objectStream.Position = 0;

            return JsonConvert.DeserializeObject<T>(
                Encoding.UTF8.GetString(serializedBytes),
                new JsonSerializerSettings {TypeNameHandling = TypeNameHandling.All});
        }

        static bool IsGzipStream(Stream stream)
        {
            if (stream == null || !stream.CanRead || !stream.CanSeek || stream.Length < FullGzipHeaderLength)
            {
                return false;
            }

            var buffer = new byte[GzipHeader.Length];
            stream.Position = 0;
            int read = stream.Read(buffer, 0, buffer.Length);
            stream.Position = 0;

            if (read != buffer.Length)
            {
                return false;
            }

            return (buffer[0] == GzipHeader[0] && buffer[1] == GzipHeader[1]);
        }

        /// <summary>
        ///     Caller disposes the returned stream
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static Stream GetCompressedStream(Stream input)
        {
            if (input == null)
            {
                return null;
            }

            var outputStream = new MemoryStream();

            using (var compressedStream = new GZipStream(outputStream, CompressionLevel.Optimal, true))
            {
                input.CopyTo(compressedStream);
            }

            outputStream.Position = 0;

            return outputStream;
        }

        /// <summary>
        ///     Caller disposes the returned stream
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        public static async Task<Stream> GetDecompressedStreamAsync(Stream input)
        {
            if (input == null)
            {
                return null;
            }

            var outputStream = new MemoryStream();

            if (IsGzipStream(input))
            {
                using (var decompressedStream = new GZipStream(input, CompressionMode.Decompress, true))
                {
                    await decompressedStream.CopyToAsync(outputStream);
                }
            }
            else
            {
                input.Position = 0;
                await input.CopyToAsync(outputStream);
            }

            outputStream.Position = 0;

            return outputStream;
        }

        public static T AsyncExceptionWrapper<T>(Func<T> method)
        {
            T retObj = default(T);
            try
            {
                retObj = method();
            }
            catch (AggregateException exception)
            {
                ExceptionDispatchInfo.Capture(exception.Flatten().InnerException).Throw();
            }
            return retObj;
        }

        public static void AsyncExceptionWrapper(Action method)
        {
            try
            {
                method();
            }
            catch (AggregateException exception)
            {
                ExceptionDispatchInfo.Capture(exception.Flatten().InnerException).Throw();
            }
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

        public static Boolean IsFatal(Exception exception)
        {
            if (exception is OutOfMemoryException || exception is StackOverflowException)
            {
                return true;
            }
            return false;
        }

        public static async Task<T> ExecuteWithRetries<T>(Func<Task<T>> retryAction, string sessionId, string operation,
            int numberOfAttempts, int delayInAttemptsSecs)
        {
            T retVal = default(T);
            int retryCount = numberOfAttempts;
            Exception lastException = null;
            while (retryCount-- > 0)
            {
                try
                {
                    retVal = await retryAction();
                    break;
                }
                catch (Exception exception)
                {
                    TraceHelper.TraceSession(TraceEventType.Warning, sessionId,
                        "Error attempting operation {0}. Attempt count = {1}. Exception: {2}\n\t{3}",
                        operation, numberOfAttempts - retryCount, exception.Message,
                        exception.StackTrace);
                    lastException = exception;
                }
                await Task.Delay(TimeSpan.FromSeconds(delayInAttemptsSecs));
            }
            if (retryCount <= 0 && lastException != null)
            {
                TraceHelper.Trace(TraceEventType.Error, "Exhausted all retries for operation " + operation);
                throw TraceHelper.TraceExceptionSession(TraceEventType.Error, sessionId, lastException);
            }
            return retVal;
        }

        public static T SyncExecuteWithRetries<T>(Func<T> retryAction, string sessionId, string operation,
            int numberOfAttempts, int delayInAttemptsSecs)
        {
            T retVal = default(T);
            int retryCount = numberOfAttempts;
            Exception lastException = null;
            while (retryCount-- > 0)
            {
                try
                {
                    retVal = retryAction();
                    break;
                }
                catch (Exception exception)
                {
                    TraceHelper.TraceSession(TraceEventType.Warning, sessionId,
                        "Error attempting operation {0}. Attempt count = {1}. Exception: {2}\n\t{3}",
                        operation, numberOfAttempts - retryCount, exception.Message,
                        exception.StackTrace);
                    lastException = exception;
                }
                Task.Delay(TimeSpan.FromSeconds(delayInAttemptsSecs)).Wait();
            }
            if (retryCount <= 0 && lastException != null)
            {
                TraceHelper.Trace(TraceEventType.Error, "Exhausted all retries for operation " + operation);
                throw TraceHelper.TraceExceptionSession(TraceEventType.Error, sessionId, lastException);
            }
            return retVal;
        }

        public static string SerializeCause(Exception originalException, DataConverter converter)
        {
            if (originalException == null)
            {
                throw new ArgumentNullException("originalException");
            }

            if (converter == null)
            {
                throw new ArgumentNullException("converter");
            }

            string details = null;
            try
            {
                details = converter.Serialize(originalException);
            }
            catch
            {
                // Cannot serialize exception, throw original exception
                throw originalException;
            }

            return details;
        }

        public static Exception RetrieveCause(string details, DataConverter converter)
        {
            if (converter == null)
            {
                throw new ArgumentNullException("converter");
            }

            Exception cause = null;
            try
            {
                if (!string.IsNullOrWhiteSpace(details))
                {
                    cause = converter.Deserialize<Exception>(details);
                }
            }
            catch (Exception converterException)
            {
                cause = new TaskFailedExceptionDeserializationException(details, converterException);
            }

            return cause;
        }

        public static string EscapeJson(string inputJson)
        {
            inputJson = inputJson.Replace("{", "{{");
            inputJson = inputJson.Replace("}", "}}");
            inputJson = inputJson.Replace(";", "%3B");
            inputJson = inputJson.Replace("=", "%3D");

            return inputJson;
        }
    }
}