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

namespace DurableTask.Test.Orchestrations
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;

    [System.Runtime.InteropServices.ComVisible(false)]
    public sealed class SimplestGetUserTask : TaskActivity<string, string>
    {
        protected override string Execute(TaskContext context, string input)
        {
            return "Gabbar";
        }
    }

    public class SimpleGenerationOrchestration : TaskOrchestration<string, string>
    {
        // HACK: This is just a hack to communicate result of orchestration back to test
        public static string Result;

        public override async Task<string> RunTask(OrchestrationContext context, string input)
        {
            string user = await context.ScheduleTask<string>(typeof(SimplestGetUserTask));
            int delayInSeconds = string.IsNullOrWhiteSpace(input) ? 0 : Int32.Parse(input);

            if (delayInSeconds > 0)
            {
                await context.CreateTimer<object>(context.CurrentUtcDateTime.Add(TimeSpan.FromSeconds(delayInSeconds)), null);
            }

            string greeting = await context.ScheduleTask<string>(typeof(SimplestSendGreetingTask), user);
            // This is a HACK to get unit test up and running.  Should never be done in actual code.
            Result = greeting;

            return greeting;
        }
    }

    public class SimplestGreetingsOrchestration : TaskOrchestration<string, string>
    {
        // HACK: This is just a hack to communicate result of orchestration back to test
        public static string Result;

        public override async Task<string> RunTask(OrchestrationContext context, string input)
        {
            string user = await context.ScheduleTask<string>(typeof(SimplestGetUserTask));
            int delayInSeconds = string.IsNullOrWhiteSpace(input) ? 0 : Int32.Parse(input);

            if (delayInSeconds > 0)
            {
                await context.CreateTimer<object>(context.CurrentUtcDateTime.Add(TimeSpan.FromSeconds(delayInSeconds)), null);
            }

            string greeting = await context.ScheduleTask<string>(typeof(SimplestSendGreetingTask), user);
            // This is a HACK to get unit test up and running.  Should never be done in actual code.
            Result = greeting;

            return greeting;
        }
    }

    public class GreetingsRepeatWaitOrchestration : TaskOrchestration<string, string>
    {
        // HACK: This is just a hack to communicate result of orchestration back to test
        public static string Result;

        public override async Task<string> RunTask(OrchestrationContext context, string input)
        {
            string user = await context.ScheduleTask<string>(typeof(SimplestGetUserTask));
            int delayInSeconds = string.IsNullOrWhiteSpace(input) ? 0 : Int32.Parse(input);

            if (delayInSeconds > 0)
            {
                for (int i = 0; i < 3; i++)
                {
                    await context.CreateTimer<object>(context.CurrentUtcDateTime.Add(TimeSpan.FromSeconds(delayInSeconds)), null);
                }
            }

            string greeting = await context.ScheduleTask<string>(typeof(SimplestSendGreetingTask), user);
            // This is a HACK to get unit test up and running.  Should never be done in actual code.
            Result = greeting;

            return greeting;
        }
    }

    public sealed class SimplestSendGreetingTask : TaskActivity<string, string>
    {
        protected override string Execute(TaskContext context, string user)
        {
            return "Greeting send to " + user;
        }
    }
    public class GenerationSignalOrchestration : TaskOrchestration<int, int>
    {
        // HACK: This is just a hack to communicate result of orchestration back to test
        public static string Result;
        public static ManualResetEvent signal = new ManualResetEvent(false);

        TaskCompletionSource<string> resumeHandle;

        public override async Task<int> RunTask(OrchestrationContext context, int numberOfGenerations)
        {
            int count = await WaitForSignal();
            signal.WaitOne();
            numberOfGenerations--;
            if (numberOfGenerations > 0)
            {
                context.ContinueAsNew(numberOfGenerations);
            }

            // This is a HACK to get unit test up and running.  Should never be done in actual code.
            Result = count.ToString();
            return count;
        }

        async Task<int> WaitForSignal()
        {
            resumeHandle = new TaskCompletionSource<string>();
            string data = await resumeHandle.Task;
            resumeHandle = null;
            return int.Parse(data);
        }

        public override void OnEvent(OrchestrationContext context, string name, string input)
        {
            if (!string.Equals("Count", name, StringComparison.Ordinal))
            {
                throw new ArgumentOutOfRangeException(nameof(name), name, "Unknown signal recieved...");
            }

            if (resumeHandle != null)
            {
                resumeHandle.SetResult(input);
            }
        }
    }

    public class ChildWorkflow : TaskOrchestration<string, int>
    {
        public override Task<string> RunTask(OrchestrationContext context, int input)
        {
            return Task.FromResult($"Child '{input}' completed.");
        }
    }

    public class ParentWorkflow : TaskOrchestration<string, bool>
    {
        // HACK: This is just a hack to communicate result of orchestration back to test
        public static string Result;

        public override async Task<string> RunTask(OrchestrationContext context, bool waitForCompletion)
        {
            var results = new Task<string>[5];
            for (int i = 0; i < 5; i++)
            {
                Task<string> r = context.CreateSubOrchestrationInstance<string>(typeof(ChildWorkflow), i);
                if (waitForCompletion)
                {
                    await r;
                }

                results[i] = r;
            }

            string[] data = await Task.WhenAll(results);
            Result = string.Concat(data);
            return Result;
        }
    }

    public class GenerationBasicOrchestration : TaskOrchestration<int, int>
    {
        // HACK: This is just a hack to communicate result of orchestration back to test
        public static int Result;

        public override async Task<int> RunTask(OrchestrationContext context, int numberOfGenerations)
        {
            int count = await context.ScheduleTask<int>(typeof(GenerationBasicTask));
            numberOfGenerations--;
            if (numberOfGenerations > 0)
            {
                context.ContinueAsNew(numberOfGenerations);
            }

            // This is a HACK to get unit test up and running.  Should never be done in actual code.
            Result = count;
            return count;
        }
    }

    public sealed class GenerationBasicTask : TaskActivity<string, int>
    {
        // HACK: This is just a hack to communicate result of orchestration back to test
        public static int GenerationCount = 0;

        protected override int Execute(TaskContext context, string input)
        {
            GenerationCount++;
            return GenerationCount;
        }
    }
}
