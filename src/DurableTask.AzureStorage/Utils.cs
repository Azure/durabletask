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
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Newtonsoft.Json;

    static class Utils
    {
        public static readonly Task CompletedTask = Task.FromResult(0);

        public static readonly string ExtensionVersion = FileVersionInfo.GetVersionInfo(typeof(AzureStorageOrchestrationService).Assembly.Location).FileVersion;

        // DurableTask.Core has a public static variable that contains the app name
        public static readonly string AppName = DurableTask.Core.Common.Utils.AppName;

        private static readonly JsonSerializer DefaultJsonSerializer = JsonSerializer.Create();

        public static async Task ParallelForEachAsync<TSource>(
            this IEnumerable<TSource> enumerable,
            Func<TSource, Task> action)
        {
            if (!enumerable.Any())
            {
                return;
            }

            var tasks = new List<Task>(32);
            foreach (TSource entry in enumerable)
            {
                tasks.Add(action(entry));
            }

            await Task.WhenAll(tasks.ToArray());
        }

        public static async Task ParallelForEachAsync<T>(this IList<T> items, int maxConcurrency, Func<T, Task> action)
        {
            if (items.Count == 0)
            {
                return;
            }

            using (var semaphore = new SemaphoreSlim(maxConcurrency))
            {
                var tasks = new Task[items.Count];
                for (int i = 0; i < items.Count; i++)
                {
                    tasks[i] = InvokeThrottledAction(items[i], action, semaphore);
                }

                await Task.WhenAll(tasks);
            }
        }

        static async Task InvokeThrottledAction<T>(T item, Func<T, Task> action, SemaphoreSlim semaphore)
        {
            await semaphore.WaitAsync();
            try
            {
                await action(item);
            }
            finally
            {
                semaphore.Release();
            }
        }

        public static double Next(this Random random, double minValue, double maxValue)
        {
            return random.NextDouble() * (maxValue - minValue) + minValue;
        }

        public static int GetEpisodeNumber(OrchestrationRuntimeState runtimeState)
        {
            return GetEpisodeNumber(runtimeState.Events);
        }

        public static int GetEpisodeNumber(IEnumerable<HistoryEvent> historyEvents)
        {
            // DTFx core writes an "OrchestratorStarted" event at the start of each episode.
            return historyEvents.Count(e => e.EventType == EventType.OrchestratorStarted);
        }

        public static int GetTaskEventId(HistoryEvent historyEvent)
        {
            if (TryGetTaskScheduledId(historyEvent, out int taskScheduledId))
            {
                return taskScheduledId;
            }

            return historyEvent.EventId;
        }

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
        /// Get the ClassName part delimited by +
        /// e.g. DurableTask.AzureStorage.Tests.Correlation.CorrelationScenarioTest+SayHelloActivity
        /// should be "SayHelloActivity"
        /// </summary>
        /// <param name="s"></param>
        public static string GetTargetClassName(this string s)
        {
            if (s == null)
            {
                return null;
            }

            int index = s.IndexOf('+');
            return s.Substring(index + 1, s.Length - index - 1);
        }

        /// <summary>
        /// Serialize some object payload to a JSON-string representation.
        /// This utility is resilient to end-user changes in the DefaultSettings of Newtonsoft.
        /// </summary>
        /// <param name="payload">The object to serialize.</param>
        /// <returns>The JSON-string representation of the payload</returns>
        public static string SerializeToJson(object payload)
        {
            return SerializeToJson(DefaultJsonSerializer, payload);
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
        public static T DeserializeFromJson<T>(JsonSerializer serializer, string jsonString)
        {
            T obj;
            using (var reader = new StringReader(jsonString))
            using (var jsonReader = new JsonTextReader(reader))
            {
                obj = serializer.Deserialize<T>(jsonReader);
            }
            return obj;
        }

        /// <summary>
        /// Deserialize a JSON-string into an object of type T
        /// This utility is resilient to end-user changes in the DefaultSettings of Newtonsoft.
        /// </summary>
        /// <typeparam name="T">The type to deserialize the JSON string into.</typeparam>
        /// <param name="stream">A stream of UTF-8 JSON.</param>
        /// <returns>The deserialized value.</returns>
        public static T DeserializeFromJson<T>(Stream stream)
        {
            return DeserializeFromJson<T>(DefaultJsonSerializer, stream);
        }

        /// <summary>
        /// Deserialize a JSON-string into an object of type T
        /// This utility is resilient to end-user changes in the DefaultSettings of Newtonsoft.
        /// </summary>
        /// <typeparam name="T">The type to deserialize the JSON string into.</typeparam>
        /// <param name="serializer">The serializer whose config will guide the deserialization.</param>
        /// <param name="stream">A stream of UTF-8 JSON.</param>
        /// <returns>The deserialized value.</returns>
        public static T DeserializeFromJson<T>(JsonSerializer serializer, Stream stream)
        {
            using var reader = new StreamReader(stream, Encoding.UTF8);
            using var jsonReader = new JsonTextReader(reader);

            return serializer.Deserialize<T>(jsonReader);
        }

        /// <summary>
        /// Deserialize a JSON-string into an object of type T
        /// This utility is resilient to end-user changes in the DefaultSettings of Newtonsoft.
        /// </summary>
        /// <typeparam name="T">The type to deserialize the JSON string into.</typeparam>
        /// <param name="jsonString">The JSON-string to deserialize.</param>
        /// <returns></returns>
        public static T DeserializeFromJson<T>(string jsonString)
        {
            return DeserializeFromJson<T>(DefaultJsonSerializer, jsonString);
        }

        /// <summary>
        /// Deserialize a JSON-string into an object of type `type`
        /// This utility is resilient to end-user changes in the DefaultSettings of Newtonsoft.
        /// </summary>
        /// <param name="jsonString">The JSON-string to deserialize.</param>
        /// <param name="type">The expected de-serialization type.</param>
        /// <returns></returns>
        public static object DeserializeFromJson(string jsonString, Type type)
        {
            return DeserializeFromJson(DefaultJsonSerializer, jsonString, type);
        }

        /// <summary>
        /// Deserialize a JSON-string into an object of type `type`
        /// This utility is resilient to end-user changes in the DefaultSettings of Newtonsoft.
        /// </summary>
        /// <param name="serializer">The serializer whose config will guide the deserialization.</param>
        /// <param name="jsonString">The JSON-string to deserialize.</param>
        /// <param name="type">The expected de-serialization type.</param>
        /// <returns></returns>
        public static object DeserializeFromJson(JsonSerializer serializer, string jsonString, Type type)
        {
            object obj;
            using (var reader = new StringReader(jsonString))
            using (var jsonReader = new JsonTextReader(reader))
            {
                obj = serializer.Deserialize(jsonReader, type);
            }
            return obj;
        }

        public static void ConvertDateTimeInHistoryEventsToUTC(HistoryEvent historyEvent)
        {
            switch (historyEvent.EventType)
            {
                case EventType.ExecutionStarted:
                    var executionStartedEvent = (ExecutionStartedEvent)historyEvent;
                    if (executionStartedEvent.ScheduledStartTime.HasValue &&
                        executionStartedEvent.ScheduledStartTime.Value.Kind != DateTimeKind.Utc)
                    {
                        executionStartedEvent.ScheduledStartTime = executionStartedEvent.ScheduledStartTime.Value.ToUniversalTime();
                    }
                    break;

                case EventType.TimerCreated:
                    var timerCreatedEvent = (TimerCreatedEvent)historyEvent;
                    if (timerCreatedEvent.FireAt.Kind != DateTimeKind.Utc)
                    {
                        timerCreatedEvent.FireAt = timerCreatedEvent.FireAt.ToUniversalTime();
                    }
                    break;

                case EventType.TimerFired:
                    var timerFiredEvent = (TimerFiredEvent)historyEvent;
                    if (timerFiredEvent.FireAt.Kind != DateTimeKind.Utc)
                    {
                        timerFiredEvent.FireAt = timerFiredEvent.FireAt.ToUniversalTime();
                    }
                    break;
            }
        }
    }
}
