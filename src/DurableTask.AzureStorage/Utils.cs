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
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;

    static class Utils
    {
        public static readonly Task CompletedTask = Task.FromResult(0);

        public static readonly string ExtensionVersion = FileVersionInfo.GetVersionInfo(typeof(AzureStorageOrchestrationService).Assembly.Location).FileVersion;

        public static async Task ParallelForEachAsync<TSource>(
            this IEnumerable<TSource> enumerable,
            Func<TSource, Task> action)
        {
            var tasks = new List<Task>(32);
            foreach (TSource entry in enumerable)
            {
                tasks.Add(action(entry));
            }

            await Task.WhenAll(tasks.ToArray());
        }

        public static async Task ParallelForEachAsync<T>(this IList<T> items, int maxConcurrency, Func<T, Task> action)
        {
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
            switch (historyEvent.EventType)
            {
                case EventType.TaskCompleted:
                    return ((TaskCompletedEvent)historyEvent).TaskScheduledId;
                case EventType.TaskFailed:
                    return ((TaskFailedEvent)historyEvent).TaskScheduledId;
                case EventType.SubOrchestrationInstanceCompleted:
                    return ((SubOrchestrationInstanceCompletedEvent)historyEvent).TaskScheduledId;
                case EventType.SubOrchestrationInstanceFailed:
                    return ((SubOrchestrationInstanceFailedEvent)historyEvent).TaskScheduledId;
                case EventType.TimerFired:
                    return ((TimerFiredEvent)historyEvent).TimerId;
                default:
                    return historyEvent.EventId;
            }
        }
    }
}
