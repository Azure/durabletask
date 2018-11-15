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
    using System.Threading;

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

        static readonly byte[] GzipHeader = { 0x1f, 0x8b };

        /// <summary>
        /// NoOp utility method
        /// </summary>
        /// <param name="parameter">The parameter.</param>
        public static void UnusedParameter(object parameter)
        {
        }

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
                new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.All }));

            objectStream.Write(serializedBytes, 0, serializedBytes.Length);
            objectStream.Position = 0;
        }

        /// <summary>
        /// Writes the supplied string input to a MemoryStream, optionally compressing the string, returns the stream
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
                Stream compressedStream = GetCompressedStream(resultStream);
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
            return ReadObjectFromByteArray<T>(ReadBytesFromStream(objectStream));
        }

        /// <summary>
        /// Reads bytes from the supplied stream
        /// </summary>
        public static byte[] ReadBytesFromStream(Stream objectStream)
        {
            if (objectStream == null || !objectStream.CanRead || !objectStream.CanSeek)
            {
                throw new ArgumentException("stream is not seekable or readable", nameof(objectStream));
            }

            objectStream.Position = 0;

            var serializedBytes = new byte[objectStream.Length];
            objectStream.Read(serializedBytes, 0, serializedBytes.Length);
            objectStream.Position = 0;

            return serializedBytes;
        }

        /// <summary>
        /// Deserializes an Object from the supplied bytes
        /// </summary>
        public static T ReadObjectFromByteArray<T>(byte[] serializedBytes)
        {
            var settings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.All,

#if NETSTANDARD2_0
                SerializationBinder = new PackageUpgradeSerializationBinder()
#else
                Binder = new PackageUpgradeSerializationBinder()
#endif
            };

            return JsonConvert.DeserializeObject<T>(
                                Encoding.UTF8.GetString(serializedBytes),
                                settings);
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
        public static bool IsFatal(Exception exception)
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
            if (numberOfAttempts == 0)
            {
                // No attempts are requested to execute the action
                return;
            }

            int retryCount = numberOfAttempts;
            ExceptionDispatchInfo lastException = null;
            while (retryCount-- > 0)
            {
                try
                {
                    await retryAction();
                    return;
                }
                catch (Exception exception) when (!IsFatal(exception))
                {
                    TraceHelper.TraceSession(
                        TraceEventType.Warning,
                        "ExecuteWithRetry-Failure",
                        sessionId,
                        $"Error attempting operation {operation}. Attempt count: {numberOfAttempts - retryCount}. Exception: {exception.Message}\n\t{exception.StackTrace}");
                    lastException = ExceptionDispatchInfo.Capture(exception);
                }

                await Task.Delay(TimeSpan.FromSeconds(delayInAttemptsSecs));
            }

            TraceHelper.Trace(TraceEventType.Error, "ExecuteWithRetry-RetriesExhausted", "Exhausted all retries for operation " + operation);
            TraceHelper.TraceExceptionSession(TraceEventType.Error, "ExecuteWithRetryRetriesExhausted", sessionId, lastException).Throw();
        }

        /// <summary>
        /// Executes the supplied action until successful or the supplied number of attempts is reached
        /// </summary>
        public static async Task<T> ExecuteWithRetries<T>(Func<Task<T>> retryAction, string sessionId, string operation,
            int numberOfAttempts, int delayInAttemptsSecs)
        {
            if (numberOfAttempts == 0)
            {
                // No attempts are requested to execute the action
                return default(T);
            }

            int retryCount = numberOfAttempts;
            ExceptionDispatchInfo lastException = null;
            while (retryCount-- > 0)
            {
                try
                {
                    return await retryAction();
                }
                catch (Exception exception) when (!IsFatal(exception))
                {
                    TraceHelper.TraceSession(
                        TraceEventType.Warning,
                        $"ExecuteWithRetry<{typeof(T)}>-Failure",
                        sessionId,
                        $"Error attempting operation {operation}. Attempt count: {numberOfAttempts - retryCount}. Exception: {exception.Message}\n\t{exception.StackTrace}");
                    lastException = ExceptionDispatchInfo.Capture(exception);
                }

                await Task.Delay(TimeSpan.FromSeconds(delayInAttemptsSecs));
            }

            string eventType = $"ExecuteWithRetry<{typeof(T)}>-Failure";
            TraceHelper.Trace(TraceEventType.Error, eventType, "Exhausted all retries for operation " + operation);

            TraceHelper.TraceExceptionSession(TraceEventType.Error, eventType, sessionId, lastException).Throw();

            // This is a noop code since TraceExceptionSession above will rethrow the cached exception however the compiler doesn't see it
            return default(T);
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

            string details;
            try
            {
                details = converter.Serialize(originalException);
            }
            catch
            {
                // Cannot serialize exception, throw original exception
                ExceptionDispatchInfo.Capture(originalException).Throw();
                throw originalException; // no op
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
            catch (Exception converterException) when (!IsFatal(converterException))
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

        /// <summary>
        /// Delay for a specified period of time with support for cancellation.
        /// </summary>
        /// <param name="timeout">The amount of time to delay.</param>
        /// <param name="cancellationToken">Token for cancelling the delay.</param>
        /// <returns>A task which completes when either the timeout expires or the cancellation token is triggered.</returns>
        public static Task DelayWithCancellation(TimeSpan timeout, CancellationToken cancellationToken)
        {
            // This implementation avoids OperationCancelledException
            // https://github.com/dotnet/corefx/issues/2704#issuecomment-131221355
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return Task.WhenAny(Task.Delay(timeout), tcs.Task);
        }
    }
}