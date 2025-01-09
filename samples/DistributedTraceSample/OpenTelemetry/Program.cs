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

namespace OpenTelemetrySample
{
    using System;
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
                .AddSource("DurableTask.Core")
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

            worker.AddTaskOrchestrations(typeof(Orchestration));
            worker.AddTaskOrchestrations(typeof(HelloSequence));
            worker.AddTaskOrchestrations(typeof(ExceptionOrchestration));
            worker.AddTaskActivities(typeof(SayHello));

            await worker.StartAsync();

            // Uncomment the next 2 lines if ErrorPropagationMode is SerializeExceptions and
            // you would like to emit exception details

            // worker.TaskActivityDispatcher.IncludeDetails = true;
            // worker.TaskOrchestrationDispatcher.IncludeDetails = true;

            TaskHubClient client = new TaskHubClient(serviceClient);

            OrchestrationInstance orchestrationInstance = await client.CreateOrchestrationInstanceAsync(
                typeof(Orchestration),
                input: null);
            await client.WaitForOrchestrationAsync(orchestrationInstance, TimeSpan.FromMinutes(5));
        }

        static async Task<(IOrchestrationService, IOrchestrationServiceClient)> GetOrchestrationServiceAndClient()
        {
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                TaskHubName = "OpenTelemetrySampleTaskHub",
                StorageAccountClientProvider = new StorageAccountClientProvider("UseDevelopmentStorage=true"),
            };

            IOrchestrationService service = new AzureStorageOrchestrationService(settings);
            IOrchestrationServiceClient client = (IOrchestrationServiceClient)service;
            await service.CreateIfNotExistsAsync();
            return (service, client);
        }

        class Orchestration : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string result = "";
                result += await context.CreateSubOrchestrationInstance<string>(typeof(HelloSequence), null);
                result += await context.ScheduleTask<string>(typeof(SayHello), "Tokyo");
                Task<string> parallelTask = context.CreateSubOrchestrationInstance<string>(typeof(ExceptionOrchestration), "orchestration threw an exception");
                result += await context.CreateSubOrchestrationInstance<string>(typeof(HelloSequence), null);
                result += await parallelTask;

                return result;
            }
        }

        class ExceptionOrchestration : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
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

        class SayHello : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                Thread.Sleep(1000);
                return $"Hello, {input}!";
            }
        }
    }
}
