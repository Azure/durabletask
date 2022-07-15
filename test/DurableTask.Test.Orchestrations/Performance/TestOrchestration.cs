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

namespace DurableTask.Test.Orchestrations.Performance
{
    using DurableTask.Core;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    public class TestOrchestration : TaskOrchestration<int, TestOrchestrationData>
    {
        public override async Task<int> RunTask(OrchestrationContext context, TestOrchestrationData data)
        {
            if (data.UseTimeoutTask)
            {
                return await RunTaskWithTimeout(context, data);
            }
            else
            {
                return await RunTaskCore(context, data);
            }
        }

        async Task<int> RunTaskCore(OrchestrationContext context, TestOrchestrationData data)
        {
            int result = 0;
            List<Task<int>> results = new List<Task<int>>();
            int i = 0;
            int j = 0;
            for (; i < data.NumberOfParallelTasks; i++)
            {
                results.Add(context.ScheduleTask<int>(
                    typeof(RandomTimeWaitingTask),
                    new RandomTimeWaitingTaskInput
                    {
                        TaskId = "ParallelTask: " + i.ToString(),
                        MaxDelay = data.MaxDelay,
                        MinDelay = data.MinDelay,
                        DelayUnit = data.DelayUnit
                    }));
            }

            int[] counters = await Task.WhenAll(results.ToArray());
            result = counters.Sum();

            for (; j < data.NumberOfSerialTasks; j++)
            {
                int c = await context.ScheduleTask<int>(typeof(RandomTimeWaitingTask),
                    new RandomTimeWaitingTaskInput
                    {
                        TaskId = "SerialTask" + (i + j),
                        MaxDelay = data.MaxDelay,
                        MinDelay = data.MinDelay,
                        DelayUnit = data.DelayUnit
                    });
                result += c;
            }

            return result;
        }

        async Task<int> RunTaskWithTimeout(OrchestrationContext context, TestOrchestrationData data)
        {
            var timerCts = new CancellationTokenSource();

            var timer = context.CreateTimer(context.CurrentUtcDateTime.Add(data.ExecutionTimeout), -1, timerCts.Token);
            var mainTask = RunTaskCore(context, data);

            var resultTask = await Task.WhenAny(mainTask, timer);

            if (resultTask == timer)
            {
                throw new TimeoutException("Orchestration timed out");
            }

            timerCts.Cancel();
            return mainTask.Result;
        }
    }
}
