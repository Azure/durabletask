﻿//  ----------------------------------------------------------------------------------
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

namespace OpenTelemetrySample
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Monitor.OpenTelemetry.Exporter;
    using DurableTask.AzureStorage;
    using DurableTask.Core;
    using OpenTelemetry;
    using OpenTelemetry.Resources;
    using OpenTelemetry.Trace;

    internal class Program
    {
        static async Task Main(string[] args)
        {
            using var tracerProvider = Sdk.CreateTracerProviderBuilder()
                .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("MySample"))
                .AddSource("DurableTask")
                .AddConsoleExporter()
                .AddZipkinExporter()
                .AddAzureMonitorTraceExporter(options =>
                {
                    options.ConnectionString = Environment.GetEnvironmentVariable("AZURE_MONITOR_CONNECTION_STRING");
                })
                .Build();

            (IOrchestrationService service, IOrchestrationServiceClient serviceClient) =
                await GetOrchestrationServiceAndClient();

            using TaskHubWorker worker = new TaskHubWorker(service);
            // worker.ErrorPropagationMode = ErrorPropagationMode.SerializeExceptions;
            worker.ErrorPropagationMode = ErrorPropagationMode.UseFailureDetails;

            worker.AddTaskOrchestrations(typeof(HelloSubOrch));
            worker.AddTaskOrchestrations(typeof(HelloSequence));
            worker.AddTaskOrchestrations(typeof(HelloFanOut));
            worker.AddTaskOrchestrations(typeof(HelloFanOutWithException));
            worker.AddTaskOrchestrations(typeof(ExceptionSubOrch));
            worker.AddTaskOrchestrations(typeof(ExceptionOrchestration));
            worker.AddTaskActivities(typeof(SayHello));
            worker.AddTaskActivities(typeof(GetRequestResultMessageActivity));
            worker.AddTaskActivities(typeof(ExceptionActivity));
            worker.AddTaskOrchestrations(typeof(GetRequestionDecisionOrchestration));
            worker.AddTaskOrchestrations(typeof(EventConversationOrchestration));
            worker.AddTaskOrchestrations(typeof(EventConversationOrchestration.Responder));
            worker.AddTaskOrchestrations(typeof(HelloSequenceWithTimer));
            worker.AddTaskOrchestrations(typeof(HelloSequenceException));

            await worker.StartAsync();

            // Uncomment the next 2 lines if ErrorPropagationMode is SerializeExceptions and
            // you would like to emit exception details

            // worker.TaskActivityDispatcher.IncludeDetails = true;
            // worker.TaskOrchestrationDispatcher.IncludeDetails = true;

            TaskHubClient client = new TaskHubClient(serviceClient);

            // Hello Sequence
            OrchestrationInstance helloSeqInstance = await client.CreateOrchestrationInstanceAsync(
                typeof(HelloSequence),
                input: null);
            await client.WaitForOrchestrationAsync(helloSeqInstance, TimeSpan.FromMinutes(5));

            Console.WriteLine("Done with Hello Sequence!");

            // Hello Sequence throws exception
            OrchestrationInstance helloSeqExceptionInstance = await client.CreateOrchestrationInstanceAsync(
                typeof(HelloSequenceException),
                input: null);
            await client.WaitForOrchestrationAsync(helloSeqExceptionInstance, TimeSpan.FromMinutes(5));

            Console.WriteLine("Done with Hello Sequence Exception!");

            // Hello Fan Out Fan In
            OrchestrationInstance helloFanOutFanInInstance = await client.CreateOrchestrationInstanceAsync(
                typeof(HelloFanOut),
                input: null);
            await client.WaitForOrchestrationAsync(helloFanOutFanInInstance, TimeSpan.FromMinutes(5));

            Console.WriteLine("Done with Hello Fan Out Fan In!");

            // Hello Fan Out Fan In with failed Activity
            OrchestrationInstance helloFanOutFanInActivityExceptionInstance = await client.CreateOrchestrationInstanceAsync(
                typeof(HelloFanOutWithException),
                input: null);
            await client.WaitForOrchestrationAsync(helloFanOutFanInActivityExceptionInstance, TimeSpan.FromMinutes(5));

            Console.WriteLine("Done with Hello Fan Out Fan In with failed Activity!");

            // Sub orchestrations
            OrchestrationInstance helloSubOrchInstance = await client.CreateOrchestrationInstanceAsync(
                typeof(HelloSubOrch),
                input: null);
            await client.WaitForOrchestrationAsync(helloSubOrchInstance, TimeSpan.FromMinutes(5));

            Console.WriteLine("Done with Hello Sub-orchestration!");

            // Sub orchestration failure
            OrchestrationInstance helloSubOrchFailedInstance = await client.CreateOrchestrationInstanceAsync(
                typeof(ExceptionSubOrch),
                input: null);
            await client.WaitForOrchestrationAsync(helloSubOrchFailedInstance, TimeSpan.FromMinutes(5));

            Console.WriteLine("Done with Hello Sub-orchestration with failed sub-orchestration!");

            // Hello Sequence with Timer
            OrchestrationInstance helloSeqWithTimerInstance = await client.CreateOrchestrationInstanceAsync(typeof(HelloSequenceWithTimer), null);
            await client.WaitForOrchestrationAsync(helloSeqWithTimerInstance, TimeSpan.FromMinutes(5));

            Console.WriteLine("Done with Hello Sequence with Timer!");

            // External event - SendEvent
            OrchestrationInstance eventConversationInstance = await client.CreateOrchestrationInstanceAsync(typeof(EventConversationOrchestration), null);
            OrchestrationState state = await client.WaitForOrchestrationAsync(eventConversationInstance, TimeSpan.FromMinutes(5));
            
            Console.WriteLine(state.Output);

            Console.WriteLine("Done with raising an external event using OrchestrationContext!");

            // External event - RaiseEvent
            OrchestrationInstance getRequestDecisionInstance = await client.CreateOrchestrationInstanceAsync(typeof(GetRequestionDecisionOrchestration), null);
            await client.RaiseEventAsync(getRequestDecisionInstance, "RequestDecision", "approved");
            OrchestrationState firstInstanceState = await client.WaitForOrchestrationAsync(getRequestDecisionInstance, TimeSpan.FromMinutes(2));

            Console.WriteLine("Result:");
            Console.WriteLine(firstInstanceState.Output);

            Console.WriteLine("Done with raising an external event with RaiseEventAsync!");
        }

        static async Task<(IOrchestrationService, IOrchestrationServiceClient)> GetOrchestrationServiceAndClient()
        {
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                TaskHubName = "OpenTelemetrySampleTaskHub",
                StorageConnectionString = "UseDevelopmentStorage=true",
            };

            IOrchestrationService service = new AzureStorageOrchestrationService(settings);
            IOrchestrationServiceClient client = (IOrchestrationServiceClient)service;
            await service.CreateIfNotExistsAsync();
            return (service, client);
        }

        class HelloSubOrch : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string result = "";
                result += await context.CreateSubOrchestrationInstance<string>(typeof(HelloSequence), null);
                result += await context.ScheduleTask<string>(typeof(SayHello), "Tokyo");
                Task<string> parallelTask = context.CreateSubOrchestrationInstance<string>(typeof(HelloFanOut), null);
                result += await context.CreateSubOrchestrationInstance<string>(typeof(HelloSequence), null);
                result += await parallelTask;

                return result;
            }
        }

        class ExceptionSubOrch : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string result = "";
                result += await context.CreateSubOrchestrationInstance<string>(typeof(HelloSequence), null);
                result += await context.ScheduleTask<string>(typeof(SayHello), "Tokyo");
                Task<string> fanOut = context.CreateSubOrchestrationInstance<string>(typeof(ExceptionOrchestration), "activity threw an exception");
                result += await context.CreateSubOrchestrationInstance<string>(typeof(HelloSequence), null);
                result += await fanOut;

                return result;
            }
        }

        class ExceptionOrchestration : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                throw new Exception(input);
            }
        }

        class HelloSequence : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string output = "";
                output += await context.ScheduleTask<string>(typeof(SayHello), "Tokyo") + ", ";
                output += await context.ScheduleTask<string>(typeof(SayHello), "London") + ", ";
                output += await context.ScheduleTask<string>(typeof(SayHello), "Seattle");
                return output;
            }
        }

        class HelloFanOut : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string[] results = await Task.WhenAll(
                    context.ScheduleTask<string>(typeof(SayHello), "Tokyo"),
                    context.ScheduleTask<string>(typeof(SayHello), "London"),
                    context.ScheduleTask<string>(typeof(SayHello), "Seattle"));
                
                return string.Join(", ", results);
            }
        }

        class HelloFanOutWithException : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string[] results = await Task.WhenAll(
                    context.ScheduleTask<string>(typeof(SayHello), "Tokyo"),
                    context.ScheduleTask<string>(typeof(ExceptionActivity), ""),
                    context.ScheduleTask<string>(typeof(SayHello), "Seattle"));

                return string.Join(", ", results);
            }
        }

        class SayHello : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                Thread.Sleep(1000);
                return $"Hello, {input}!";
            }
        }

        class HelloSequenceException : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string output = "";
                output += await context.ScheduleTask<string>(typeof(SayHello), "Tokyo") + ", ";
                output += await context.ScheduleTask<string>(typeof(SayHello), "London") + ", ";
                output += await context.ScheduleTask<string>(typeof(ExceptionActivity), "This is an invalid operation.") + ", ";
                output += await context.ScheduleTask<string>(typeof(SayHello), "Seattle");
                return output;
            }
        }
        class ExceptionActivity : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                throw new InvalidOperationException(input);
            }
        }

        class HelloSequenceWithTimer : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string output = "";
                output += await context.ScheduleTask<string>(typeof(SayHello), "Tokyo") + ", ";
                await context.CreateTimer(context.CurrentUtcDateTime.AddSeconds(15), true);
                output += await context.ScheduleTask<string>(typeof(SayHello), "London") + ", ";
                output += await context.ScheduleTask<string>(typeof(SayHello), "Seattle");
                return output;
            }
        }

        class GetRequestResultMessageActivity : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                switch (input)
                {
                    case "approved":
                        return "Approved request!";
                    case "declined":
                        return "Declined request! Please submit another requst.";
                    default:
                        return "Unable to understand input. Please try again.";
                }
            }
        }

        public class GetRequestionDecisionOrchestration : TaskOrchestration<string, string>
        {
            TaskCompletionSource<object> getPermission = new TaskCompletionSource<object>();

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string decision = (string)await getPermission.Task;
                string result = await context.ScheduleTask<string>(typeof(GetRequestResultMessageActivity), decision);
                return result;
            }

            public override void OnEvent(OrchestrationContext context, string name, string input)
            {
                getPermission.SetResult(input);
            }
        }

        public sealed class EventConversationOrchestration : TaskOrchestration<string, bool>
        {
            private readonly TaskCompletionSource<string> tcs1
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
                    responderOrchestration = context.CreateSubOrchestrationInstance<string>(typeof(Responder), responderId, "Seattle");
                }
                else
                {
                    var dummyTask = context.CreateSubOrchestrationInstance<object>(NameVersionHelper.GetDefaultName(typeof(Responder)), "", responderId, "Seattle",
                        new Dictionary<string, string>() { { OrchestrationTags.FireAndForget, "" } });

                    if (!dummyTask.IsCompleted)
                    {
                        throw new Exception("test failed: fire-and-forget should complete immediately");
                    }

                    responderOrchestration = Task.FromResult("Bye from Seattle");
                }

                // send the id of this orchestration to the responder
                var responderInstance = new OrchestrationInstance() { InstanceId = responderId };
                context.SendEvent(responderInstance, channelName, context.OrchestrationInstance.InstanceId);

                // wait for a response event 
                var message = await tcs1.Task;
                if (message != "Hi from Seattle")
                    throw new Exception("test failed");

                // tell the responder to stop listening
                context.SendEvent(responderInstance, channelName, "stop");

                // if this was not a fire-and-forget orchestration, wait for it to complete
                var receiverResult = await responderOrchestration;

                if (receiverResult != "Bye from Seattle")
                    throw new Exception("test failed");

                OkResult = true;

                return "OK";
            }

            public override void OnEvent(OrchestrationContext context, string name, string input)
            {
                if (name == channelName)
                {
                    tcs1.TrySetResult(input);
                }
            }

            private const string channelName = "conversation";

            public class Responder : TaskOrchestration<string, string>
            {
                private readonly TaskCompletionSource<string> tcs2
                    = new TaskCompletionSource<string>(TaskContinuationOptions.ExecuteSynchronously);

                public async override Task<string> RunTask(OrchestrationContext context, string input)
                {
                    var message = await tcs2.Task;

                    if (message == "stop")
                    {
                        return $"Bye from {input}";
                    }
                    else
                    {
                        // send a message back to the sender
                        var senderInstance = new OrchestrationInstance() { InstanceId = message };
                        context.SendEvent(senderInstance, channelName, $"Hi from {input}");

                        // start over to wait for the next message
                        context.ContinueAsNew(input);

                        return "this value is meaningless";
                    }
                }

                public override void OnEvent(OrchestrationContext context, string name, string input)
                {
                    if (name == channelName)
                    {
                        tcs2.TrySetResult(input);
                    }
                }
            }
        }
    }
}
