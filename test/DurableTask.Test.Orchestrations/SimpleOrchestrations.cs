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
    using System.Collections.Generic;
    using System.Runtime.Serialization;
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
                for (var i = 0; i < 3; i++)
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
        public static ManualResetEvent Signal = new ManualResetEvent(false);

        TaskCompletionSource<string> resumeHandle;

        public override async Task<int> RunTask(OrchestrationContext context, int numberOfGenerations)
        {
            int count = await WaitForSignal();
            Signal.WaitOne();
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
            this.resumeHandle = new TaskCompletionSource<string>();
            string data = await this.resumeHandle.Task;
            this.resumeHandle = null;
            return int.Parse(data);
        }

        public override void OnEvent(OrchestrationContext context, string name, string input)
        {
            if (!string.Equals("Count", name, StringComparison.Ordinal))
            {
                throw new ArgumentOutOfRangeException(nameof(name), name, "Unknown signal recieved...");
            }

            this.resumeHandle?.SetResult(input);
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

    public sealed class CounterOrchestration : TaskOrchestration<int, int>
    {
        TaskCompletionSource<string> waitForOperationHandle;

        public override async Task<int> RunTask(OrchestrationContext context, int currentValue)
        {
            string operation = await this.WaitForOperation();

            bool done = false;
            switch (operation?.ToLowerInvariant())
            {
                case "incr":
                    currentValue++;
                    break;
                case "decr":
                    currentValue--;
                    break;
                case "end":
                    done = true;
                    break;
            }

            if (!done)
            {
                context.ContinueAsNew(currentValue);
            }

            return currentValue;

        }

        async Task<string> WaitForOperation()
        {
            this.waitForOperationHandle = new TaskCompletionSource<string>();
            string operation = await this.waitForOperationHandle.Task;
            this.waitForOperationHandle = null;
            return operation;
        }

        public override void OnEvent(OrchestrationContext context, string name, string input)
        {
            if (this.waitForOperationHandle != null && !this.waitForOperationHandle.Task.IsCompleted)
            {
                this.waitForOperationHandle.SetResult(input);
            }
        }
    }

    public sealed class ContinueAsNewThenTimerOrchestration : TaskOrchestration<string,int>
    {
        public override async Task<string> RunTask(OrchestrationContext context, int input)
        {
            if (input == 0)
            {
                context.ContinueAsNew(1);

                return "continue as new";
            }
            else if (input == 1)
            {
                await context.CreateTimer(context.CurrentUtcDateTime, 0);

                return "OK";
            }
            else
            {
                throw new InvalidOperationException();
            }
        }
    }

    [KnownType(typeof(EventConversationOrchestration.Responder))]
    public sealed class EventConversationOrchestration : TaskOrchestration<string, bool>
    {
        private readonly TaskCompletionSource<string> tcs
            = new TaskCompletionSource<string>(TaskContinuationOptions.ExecuteSynchronously);

        // HACK: This is just a hack to communicate result of orchestration back to test
        public static bool OkResult;

        public async override Task<string> RunTask(OrchestrationContext context, bool useFireAndForgetSubOrchestration)
        {
            // start a responder orchestration
            var responderId = "responderId";
            Task<string> responderOrchestration = null;

            if (!useFireAndForgetSubOrchestration)
            {
                responderOrchestration = context.CreateSubOrchestrationInstance<string>(typeof(Responder), responderId, "Herkimer");
            }
            else
            {
                var dummyTask = context.CreateSubOrchestrationInstance<object>(NameVersionHelper.GetDefaultName(typeof(Responder)), "", responderId, "Herkimer",
                    new Dictionary<string, string>() { { OrchestrationTags.FireAndForget, "" } });

                if (!dummyTask.IsCompleted)
                {
                    throw new Exception("test failed: fire-and-forget should complete immediately");
                }

                responderOrchestration = Task.FromResult("Herkimer is done");
            }

            // send the id of this orchestration to the responder
            var responderInstance = new OrchestrationInstance() { InstanceId = responderId };
            context.SendEvent(responderInstance, channelName, context.OrchestrationInstance.InstanceId);

            // wait for a response event 
            var message = await tcs.Task;
            if (message != "hi from Herkimer")
                throw new Exception("test failed");

            // tell the responder to stop listening
            context.SendEvent(responderInstance, channelName, "stop");

            // if this was not a fire-and-forget orchestration, wait for it to complete
            var receiverResult = await responderOrchestration;

            if (receiverResult != "Herkimer is done")
                throw new Exception("test failed");

            OkResult = true;

            return "OK";
        }

        public override void OnEvent(OrchestrationContext context, string name, string input)
        {
            if (name == channelName)
            {
                tcs.TrySetResult(input);
            }
        }

        private const string channelName = "conversation";

        public class Responder : TaskOrchestration<string, string>
        {
            private readonly TaskCompletionSource<string> tcs
                = new TaskCompletionSource<string>(TaskContinuationOptions.ExecuteSynchronously);

            public async override Task<string> RunTask(OrchestrationContext context, string input)
            {
                var message = await tcs.Task;

                if (message == "stop")
                {
                    return $"{input} is done";
                }
                else
                {
                    // send a message back to the sender
                    var senderInstance = new OrchestrationInstance() { InstanceId = message };
                    context.SendEvent(senderInstance, channelName, $"hi from {input}");

                    // start over to wait for the next message
                    context.ContinueAsNew(input);

                    return "this value is meaningless";
                }
            }

            public override void OnEvent(OrchestrationContext context, string name, string input)
            {
                if (name == channelName)
                {
                    tcs.TrySetResult(input);
                }
            }
        }
    }

}
