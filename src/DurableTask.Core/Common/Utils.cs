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

namespace DurableTask.Core.Common
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.IO.Compression;
    using System.Runtime.ExceptionServices;
    using System.Text;
    using System.Threading.Tasks;
    using Newtonsoft.Json;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.Serializing;
    using Tracing;

    /// <summary>
    /// Utility Methods
    /// </summary>
    public static class Utils
    {
        const int FullGzipHeaderLength = 10;

        /// <summary>
        /// Gets a safe maximum datetime value that accounts for timezone
        /// </summary>
        public static readonly DateTime DateTimeSafeMaxValue =
            DateTime.MaxValue.Subtract(TimeSpan.FromDays(1)).ToUniversalTime();

        static readonly byte[] GzipHeader = {0x1f, 0x8b};

        /// <summary>
        /// Extension method to truncate a string to the supplied length
        /// </summary>
        public static string Truncate(this string input, int maxLength)
        {
            if (!string.IsNullOrEmpty(input) && input.Length > maxLength)
            {
                return input.Substring(0, maxLength);
            }
            return input;
        }

        /// <summary>
        /// Serializes and appends the supplied object to the supplied stream
        /// </summary>
        public static void WriteObjectToStream(Stream objectStream, object obj)
        {
            if (objectStream == null || !objectStream.CanWrite || !objectStream.CanSeek)
            {
                throw new ArgumentException("stream is not seekable or writable", nameof(objectStream));
            }

            byte[] serializedBytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(obj,
                new JsonSerializerSettings {TypeNameHandling = TypeNameHandling.All}));

            objectStream.Write(serializedBytes, 0, serializedBytes.Length);
            objectStream.Position = 0;
        }

        /// <summary>
        /// Writes the supplied string input to a MemoryStream, optionaly compressing the string, returns the stream
        /// </summary>
        public static Stream WriteStringToStream(string input, bool compress, out long originalStreamSize)
        {
            Stream resultStream = new MemoryStream();

            byte[] bytes = Encoding.UTF8.GetBytes(input);

            resultStream.Write(bytes, 0, bytes.Length);
            resultStream.Position = 0;
            originalStreamSize = resultStream.Length;

            if (compress)
            {
                var compressedStream = GetCompressedStream(resultStream);
                resultStream.Dispose();
                resultStream = compressedStream;
            }

            return resultStream;
        }

        /// <summary>
        /// Reads and deserializes an Object from the supplied stream
        /// </summary>
        public static T ReadObjectFromStream<T>(Stream objectStream)
        {
            if (objectStream == null || !objectStream.CanRead || !objectStream.CanSeek)
            {
                throw new ArgumentException("stream is not seekable or readable", nameof(objectStream));
            }

            objectStream.Position = 0;

            var serializedBytes = new byte[objectStream.Length];
            objectStream.Read(serializedBytes, 0, serializedBytes.Length);
            objectStream.Position = 0;

            return JsonConvert.DeserializeObject<T>(
                Encoding.UTF8.GetString(serializedBytes),
                new JsonSerializerSettings {TypeNameHandling = TypeNameHandling.All});
        }

        /// <summary>
        /// Returns true or false whether the supplied stream is a compressed stream
        /// </summary>
        public static bool IsGzipStream(Stream stream)
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

        /// <summary>
        /// Returns true or false whether an exception is considered fatal
        /// </summary>
        public static Boolean IsFatal(Exception exception)
        {
            if (exception is OutOfMemoryException || exception is StackOverflowException)
            {
                return true;
            }
            return false;
        }

        /// <summary>
        /// Executes the supplied action until successful or the supplied number of attempts is reached
        /// </summary>
        public static async Task ExecuteWithRetries(Func<Task> retryAction, string sessionId, string operation,
            int numberOfAttempts, int delayInAttemptsSecs)
        {
            int retryCount = numberOfAttempts;
            Exception lastException = null;
            while (retryCount-- > 0)
            {
                try
                {
                    await retryAction();
                    break;
                }
                catch (Exception exception) when (!Utils.IsFatal(exception))
                {
                    TraceHelper.TraceSession(
                        TraceEventType.Warning, 
                        "ExecuteWithRetry-Failure", 
                        sessionId,
                        $"Error attempting operation {operation}. Attempt count: {numberOfAttempts - retryCount}. Exception: {exception.Message}\n\t{exception.StackTrace}");
                    lastException = exception;
                }

                await Task.Delay(TimeSpan.FromSeconds(delayInAttemptsSecs));
            }

            if (retryCount <= 0 && lastException != null)
            {
                TraceHelper.Trace(TraceEventType.Error, "ExecuteWithRetry-RetriesExhausted", "Exhausted all retries for operation " + operation);
                throw TraceHelper.TraceExceptionSession(TraceEventType.Error, "ExecuteWithRetryRetriesExhausted", sessionId, lastException);
            }
        }

        /// <summary>
        /// Executes the supplied action until successful or the supplied number of attempts is reached
        /// </summary>
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
                catch (Exception exception) when (!Utils.IsFatal(exception))
                {
                    TraceHelper.TraceSession(
                        TraceEventType.Warning, 
                        $"ExecuteWithRetry<{typeof(T)}>-Failure", 
                        sessionId,
                        $"Error attempting operation {operation}. Attempt count: {numberOfAttempts - retryCount}. Exception: {exception.Message}\n\t{exception.StackTrace}");
                    lastException = exception;
                }

                await Task.Delay(TimeSpan.FromSeconds(delayInAttemptsSecs));
            }

            if (retryCount <= 0 && lastException != null)
            {
                var eventType = $"ExecuteWithRetry<{typeof(T)}>-Failure";
                TraceHelper.Trace(TraceEventType.Error, eventType, "Exhausted all retries for operation " + operation);
                throw TraceHelper.TraceExceptionSession(TraceEventType.Error, eventType, sessionId, lastException);
            }

            return retVal;
        }


        /// <summary>
        /// Serializes the supplied exception to a string
        /// </summary>
        public static string SerializeCause(Exception originalException, DataConverter converter)
        {
            if (originalException == null)
            {
                throw new ArgumentNullException(nameof(originalException));
            }

            if (converter == null)
            {
                throw new ArgumentNullException(nameof(converter));
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

        /// <summary>
        /// Retrieves the exception from a previously serialized exception
        /// </summary>
        public static Exception RetrieveCause(string details, DataConverter converter)
        {
            if (converter == null)
            {
                throw new ArgumentNullException(nameof(converter));
            }

            Exception cause = null;
            try
            {
                if (!string.IsNullOrWhiteSpace(details))
                {
                    cause = converter.Deserialize<Exception>(details);
                }
            }
            catch (Exception converterException) when (!Utils.IsFatal(converterException))
            {
                cause = new TaskFailedExceptionDeserializationException(details, converterException);
            }

            return cause;
        }

        /// <summary>
        /// Escapes the supplied input
        /// </summary>
        public static string EscapeJson(string inputJson)
        {
            inputJson = inputJson.Replace("{", "{{");
            inputJson = inputJson.Replace("}", "}}");
            inputJson = inputJson.Replace(";", "%3B");
            inputJson = inputJson.Replace("=", "%3D");

            return inputJson;
        }

        /// <summary>
        /// Builds a new OrchestrationState from the supplied OrchestrationRuntimeState
        /// </summary>
        public static OrchestrationState BuildOrchestrationState(OrchestrationRuntimeState runtimeState)
        {
            return new OrchestrationState
            {
                OrchestrationInstance = runtimeState.OrchestrationInstance,
                ParentInstance = runtimeState.ParentInstance,
                Name = runtimeState.Name,
                Version = runtimeState.Version,
                Status = runtimeState.Status,
                Tags = runtimeState.Tags,
                OrchestrationStatus = runtimeState.OrchestrationStatus,
                CreatedTime = runtimeState.CreatedTime,
                CompletedTime = runtimeState.CompletedTime,
                LastUpdatedTime = DateTime.UtcNow,
                Size = runtimeState.Size,
                CompressedSize = runtimeState.CompressedSize,
                Input = runtimeState.Input,
                Output = runtimeState.Output
            };
        }
    }
}