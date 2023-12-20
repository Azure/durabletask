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
#nullable enable
namespace DurableTask.Core.Common
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.IO.Compression;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.ExceptionServices;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Serializing;
    using DurableTask.Core.Tracing;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

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
        /// Gets the version of the DurableTask.Core nuget package, which by convension is the same as the assembly file version.
        /// </summary>
        internal static readonly string PackageVersion = FileVersionInfo.GetVersionInfo(typeof(TaskOrchestration).Assembly.Location).FileVersion;

        private static readonly JsonSerializerSettings ObjectJsonSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.All,

#if NETSTANDARD2_0
            SerializationBinder = new PackageUpgradeSerializationBinder()
#else
            Binder = new PackageUpgradeSerializationBinder()
#endif
        };
        private static readonly JsonSerializer DefaultObjectJsonSerializer = JsonSerializer.Create(ObjectJsonSettings);

        private static readonly JsonSerializer DefaultSerializer = JsonSerializer.Create();

        /// <summary>
        /// Serialize some object payload to a JSON-string representation.
        /// This utility is resilient to end-user changes in the DefaultSettings of Newtonsoft.
        /// </summary>
        /// <param name="payload">The object to serialize.</param>
        /// <returns>The JSON-string representation of the payload</returns>
        public static string SerializeToJson(object payload)
        {
            return SerializeToJson(DefaultSerializer, payload);
        }

        /// <summary>
        /// Serialize some object payload to a JSON-string representation.
        /// This utility is resilient to end-user changes in the DefaultSettings of Newtonsoft.
        /// </summary>
        /// <param name="serializer">The serializer to use.</param>
        /// <param name="payload">The object to serialize.</param>
        /// <returns>The JSON-string representation of the payload</returns>
        public static string SerializeToJson(JsonSerializer serializer, object payload)
        {
            StringBuilder stringBuilder = new StringBuilder();
            using (var stringWriter = new StringWriter(stringBuilder))
            {
                serializer.Serialize(stringWriter, payload);
            }
            var jsonStr = stringBuilder.ToString();
            return jsonStr;
        }

        /// <summary>
        /// Deserialize a JSON-string into an object of type T
        /// This utility is resilient to end-user changes in the DefaultSettings of Newtonsoft.
        /// </summary>
        /// <typeparam name="T">The type to deserialize the JSON string into.</typeparam>
        /// <param name="serializer">The serializer whose config will guide the deserialization.</param>
        /// <param name="jsonString">The JSON-string to deserialize.</param>
        /// <returns></returns>
        public static T? DeserializeFromJson<T>(JsonSerializer serializer, string jsonString)
        {
            T? obj;
            using (var reader = new StringReader(jsonString))
            using (var jsonReader = new JsonTextReader(reader))
            {
                obj = serializer.Deserialize<T>(jsonReader);
            }
            return obj;
        }

        /// <summary>
        /// Deserialize a JSON-string into an object of type `type`
        /// This utility is resilient to end-user changes in the DefaultSettings of Newtonsoft.
        /// </summary>
        /// <param name="jsonString">The JSON-string to deserialize.</param>
        /// <param name="type">The expected de-serialization type.</param>
        /// <returns></returns>
        public static object? DeserializeFromJson(string jsonString, Type type)
        {
            return DeserializeFromJson(DefaultSerializer, jsonString, type);
        }

        /// <summary>
        /// Deserialize a JSON-string into an object of type `type`
        /// This utility is resilient to end-user changes in the DefaultSettings of Newtonsoft.
        /// </summary>
        /// <param name="serializer">The serializer whose config will guide the deserialization.</param>
        /// <param name="jsonString">The JSON-string to deserialize.</param>
        /// <param name="type">The expected de-serialization type.</param>
        /// <returns></returns>
        public static object? DeserializeFromJson(JsonSerializer serializer, string jsonString, Type type)
        {
            object? obj;
            using (var reader = new StringReader(jsonString))
            using (var jsonReader = new JsonTextReader(reader))
            {
                obj = serializer.Deserialize(jsonReader, type);
            }
            return obj;
        }


        /// <summary>
        /// Gets or sets the name of the app, for use when writing structured event source traces.
        /// </summary>
        /// <remarks>
        /// The default value comes from the WEBSITE_SITE_NAME environment variable, which is defined
        /// in Azure App Service. Other environments can use DTFX_APP_NAME to set this value.
        /// </remarks>
        public static string AppName { get; set; } =
            Environment.GetEnvironmentVariable("WEBSITE_SITE_NAME") ??
            Environment.GetEnvironmentVariable("DTFX_APP_NAME") ??
            string.Empty;

        /// <summary>
        /// Gets or sets a value indicating whether to redact user code exceptions from log output.
        /// </summary>
        /// <remarks>
        /// This value defaults to <c>true</c> if the WEBSITE_SITE_NAME is set to a non-empty value, which is always
        /// the case in hosted Azure environments where log telemetry is automatically captured. This value can be also
        /// be set explicitly by assigning the DTFX_REDACT_USER_CODE_EXCEPTIONS environment variable to "1" or "true".
        /// </remarks>
        public static bool RedactUserCodeExceptions { get; set; } = GetRedactUserCodeExceptionsDefaultValue();

        static bool GetRedactUserCodeExceptionsDefaultValue()
        {
            string? configuredValue = Environment.GetEnvironmentVariable("DTFX_REDACT_USER_CODE_EXCEPTIONS")?.Trim();
            if (configuredValue != null)
            {
                return configuredValue == "1" || configuredValue.Equals("true", StringComparison.OrdinalIgnoreCase);
            }
            else
            {
                // Fallback case when DTFX_REDACT_USER_CODE_EXCEPTIONS is not defined is to automatically redact
                // any time we appear to be in a hosted Azure environment.
                return Environment.GetEnvironmentVariable("WEBSITE_SITE_NAME") != null;
            }
        }

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

        internal static JArray ConvertToJArray(string input)
        {
            JArray jArray;
            using (var stringReader = new StringReader(input))
            using (var jsonTextReader = new JsonTextReader(stringReader) { DateParseHandling = DateParseHandling.None })
            {
                jArray = JArray.Load(jsonTextReader);
            }

            return jArray;
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

            var jsonStr = SerializeToJson(DefaultObjectJsonSerializer, obj);
            byte[] serializedBytes = Encoding.UTF8.GetBytes(jsonStr);

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
                Stream compressedStream = GetCompressedStream(resultStream)!;
                resultStream.Dispose();
                resultStream = compressedStream;
            }

            return resultStream;
        }

        /// <summary>
        /// Reads and deserializes an Object from the supplied stream
        /// </summary>
        public static T? ReadObjectFromStream<T>(Stream objectStream)
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
        public static T? ReadObjectFromByteArray<T>(byte[] serializedBytes)
        {
            var jsonString = Encoding.UTF8.GetString(serializedBytes);
            return DeserializeFromJson<T>(DefaultObjectJsonSerializer, jsonString);
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
        public static Stream? GetCompressedStream(Stream input)
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
        public static async Task<Stream?> GetDecompressedStreamAsync(Stream input)
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
        /// Returns true if an exception represents an aborting execution; false otherwise.
        /// </summary>
        public static bool IsExecutionAborting(Exception exception) => exception is SessionAbortedException;

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
            ExceptionDispatchInfo? lastException = null;
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
            TraceHelper.TraceExceptionSession(TraceEventType.Error, "ExecuteWithRetryRetriesExhausted", sessionId, lastException!).Throw();
        }

        /// <summary>
        /// Executes the supplied action until successful or the supplied number of attempts is reached
        /// </summary>
        public static async Task<T?> ExecuteWithRetries<T>(Func<Task<T>> retryAction, string sessionId, string operation,
            int numberOfAttempts, int delayInAttemptsSecs)
        {
            if (numberOfAttempts == 0)
            {
                // No attempts are requested to execute the action
                return default(T);
            }

            int retryCount = numberOfAttempts;
            ExceptionDispatchInfo? lastException = null;
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

            TraceHelper.TraceExceptionSession(TraceEventType.Error, eventType, sessionId, lastException!).Throw();

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
        public static Exception? RetrieveCause(string details, DataConverter converter)
        {
            if (converter == null)
            {
                throw new ArgumentNullException(nameof(converter));
            }

            Exception? cause = null;
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
                Output = runtimeState.Output,
                ScheduledStartTime = runtimeState.ExecutionStartedEvent?.ScheduledStartTime,
                FailureDetails = runtimeState.FailureDetails,
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

        /// <summary>
        /// Gets the task event ID for the specified <see cref="HistoryEvent"/>.
        /// </summary>
        /// <param name="historyEvent">The history which may or may not contain a task event ID.</param>
        /// <returns>Returns the task event ID or <c>-1</c> if none exists.</returns>
        public static int GetTaskEventId(HistoryEvent historyEvent)
        {
            if (TryGetTaskScheduledId(historyEvent, out int taskScheduledId))
            {
                return taskScheduledId;
            }

            return historyEvent.EventId;
        }

        /// <summary>
        /// Gets the task event ID for the specified <see cref="HistoryEvent"/> if one exists.
        /// </summary>
        /// <param name="historyEvent">The history which may or may not contain a task event ID.</param>
        /// <param name="taskScheduledId">The task event ID or <c>-1</c> if none exists.</param>
        /// <returns>Returns <c>true</c> if a task event ID was found; <c>false</c> otherwise.</returns>
        public static bool TryGetTaskScheduledId(HistoryEvent historyEvent, out int taskScheduledId)
        {
            switch (historyEvent.EventType)
            {
                case EventType.TaskCompleted:
                    taskScheduledId = ((TaskCompletedEvent)historyEvent).TaskScheduledId;
                    return true;
                case EventType.TaskFailed:
                    taskScheduledId = ((TaskFailedEvent)historyEvent).TaskScheduledId;
                    return true;
                case EventType.SubOrchestrationInstanceCompleted:
                    taskScheduledId = ((SubOrchestrationInstanceCompletedEvent)historyEvent).TaskScheduledId;
                    return true;
                case EventType.SubOrchestrationInstanceFailed:
                    taskScheduledId = ((SubOrchestrationInstanceFailedEvent)historyEvent).TaskScheduledId;
                    return true;
                case EventType.TimerFired:
                    taskScheduledId = ((TimerFiredEvent)historyEvent).TimerId;
                    return true;
                default:
                    taskScheduledId = -1;
                    return false;
            }
        }

        /// <summary>
        /// Creates a determinstic Guid from a string using a hash function. This is a simple hash
        /// meant to produce pseudo-random Guids, it is not meant to be cryptographically secure, 
        /// and does not follow any formatting conventions for UUIDs (such as RFC 4122).
        /// </summary>
        /// <param name="stringToHash">The string to hash.</param>
        /// <returns>A Guid constructed from the hash.</returns>
        /// <exception cref="ArgumentException"></exception>
        internal static Guid CreateGuidFromHash(string stringToHash)
        {
            if (string.IsNullOrEmpty(stringToHash))
            {
                throw new ArgumentException("string to hash must not be null or empty", nameof(stringToHash));
            }

            var bytes = Encoding.UTF8.GetBytes(stringToHash);
            uint hash1 = Fnv1aHashHelper.ComputeHash(bytes, 0xdf0dd395);
            uint hash2 = Fnv1aHashHelper.ComputeHash(bytes, 0xa19df4df);
            uint hash3 = Fnv1aHashHelper.ComputeHash(bytes, 0xc88599c5);
            uint hash4 = Fnv1aHashHelper.ComputeHash(bytes, 0xe24e3e64);
            return new Guid(
                hash1,
                (ushort)(hash2 & 0xFFFF),
                (ushort)((hash2 >> 16) & 0xFFFF),
                (byte)(hash3 & 0xFF),
                (byte)((hash3 >> 8) & 0xFF),
                (byte)((hash3 >> 16) & 0xFF),
                (byte)((hash3 >> 24) & 0xFF),
                (byte)(hash4 & 0xFF),
                (byte)((hash4 >> 8) & 0xFF),
                (byte)((hash4 >> 16) & 0xFF),
                (byte)((hash4 >> 24) & 0xFF));
        }

        /// <summary>
        /// Gets the generic return type for a specific <paramref name="methodInfo"/>.
        /// </summary>
        /// <param name="methodInfo">The method to get the generic return type for.</param>
        /// <param name="genericArguments">The generic method arguments.</param>
        internal static Type GetGenericReturnType(MethodInfo methodInfo, Type[] genericArguments)
        {
            if (!methodInfo.ReturnType.IsGenericType)
            {
                throw new InvalidOperationException("Return type is not a generic type. Type Name: " + methodInfo.ReturnType.FullName);
            }

            Type genericArgument = methodInfo.ReturnType.GetGenericArguments().SingleOrDefault() ??
                throw new NotSupportedException($"The method {methodInfo.Name} cannot be used because its return type '{methodInfo.ReturnType.FullName}' has more than one generic parameter."); ;

            return ConvertFromGenericType(genericParameters: methodInfo.GetGenericArguments(), genericArguments, genericArgument);
        }

        /// <summary>
        /// Converts the specified <paramref name="typeToConvert"/> to a non-generic equivalent.
        /// </summary>
        /// <param name="genericParameters">The generic type parameters.</param>
        /// <param name="genericArguments">The generic type arguments.</param>
        /// <param name="typeToConvert">The type to convert.</param>
        /// <returns>The non-generic representation of the type.</returns>
        /// <remarks>
        /// A type can exist in one of the following states;
        /// 1. T[]: Array with generic element type
        /// 2. Concrete<![CDATA[<T>]]>: A concrete type with generic type args e.g List<![CDATA[<T>]]>.
        /// 3. T: A generic parameter.
        /// 4. Concrete: A simple, non-generic type.
        /// </remarks>
        internal static Type ConvertFromGenericType(Type[] genericParameters, Type[] genericArguments, Type typeToConvert)
        {
            // Check if type is of form T[]
            if (typeToConvert.IsArray)
            {
                Type elementType = typeToConvert.GetElementType();
                if (elementType.IsGenericParameter)
                {
                    int index = Array.IndexOf(genericParameters, elementType);

                    // Return the value of the generic argument.
                    return ConvertFromGenericType(
                        genericParameters,
                        genericArguments,
                        genericArguments[index].MakeArrayType());
                }
            }

            // Check if type if of form Concrete<T> e.g Dictionary<T, U>
            if (typeToConvert.IsGenericType)
            {
                Type[] genericArgs = typeToConvert.GetGenericArguments();
                List<Type> genericTypeValues = new List<Type>();

                foreach (Type genericArg in genericArgs)
                {
                    // Return the value of the generic argument.
                    genericTypeValues.Add(ConvertFromGenericType(genericParameters, genericArguments, genericArg));
                }

                return typeToConvert.GetGenericTypeDefinition().MakeGenericType(genericTypeValues.ToArray());
            }

            // Check if type is of form T
            if (typeToConvert.IsGenericParameter)
            {
                int index = Array.IndexOf(genericParameters, typeToConvert);

                // Return the value of the generic argument.
                return ConvertFromGenericType(
                    genericParameters,
                    genericArguments,
                    genericArguments[index]);
            }

            // return since the argument is a concrete type.
            return typeToConvert;
        }

        internal sealed class TypeMetadata
        {
            public string? AssemblyName { get; set; }

            public string? FullyQualifiedTypeName { get; set; }
        }
    }
}
