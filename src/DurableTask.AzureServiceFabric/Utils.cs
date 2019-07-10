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

namespace DurableTask.AzureServiceFabric
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    using DurableTask.Core;
    using DurableTask.AzureServiceFabric.Tracing;

    internal static class Utils
    {
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

        public static TimeSpan Measure(Action action)
        {
            var timer = Stopwatch.StartNew();
            action();
            timer.Stop();
            return timer.Elapsed;
        }

        public static async Task<TimeSpan> MeasureAsync(Func<Task> asyncAction)
        {
            var timer = Stopwatch.StartNew();
            await asyncAction();
            timer.Stop();
            return timer.Elapsed;
        }

        public static async Task RunBackgroundJob(Func<Task> loopAction, TimeSpan initialDelay, TimeSpan delayOnSuccess, TimeSpan delayOnException, string actionName, CancellationToken token)
        {
            try
            {
                await Task.Delay(initialDelay, token);
            }
            catch(TaskCanceledException)
            {
            }

            while (!token.IsCancellationRequested)
            {
                try
                {
                    await loopAction();
                    await Task.Delay(delayOnSuccess).ConfigureAwait(false);
                }
                catch(Exception e)
                {
                    ServiceFabricProviderEventSource.Tracing.ExceptionWhileRunningBackgroundJob(actionName, e.ToString());
                    await Task.Delay(delayOnException).ConfigureAwait(false);
                }
            }
        }
    }
}
