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
            worker.AddTaskOrchestrations(typeof(HelloSequence));
            worker.AddTaskOrchestrations(typeof(HelloFanOut));
            worker.AddTaskActivities(typeof(SayHello));
            await worker.StartAsync();

            TaskHubClient client = new TaskHubClient(serviceClient);
            OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(
                typeof(HelloFanOut),
                //typeof(HelloSequence),
                input: null);
            await client.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(5));

            Console.WriteLine("Done!");
        }

        static async Task<(IOrchestrationService, IOrchestrationServiceClient)> GetOrchestrationServiceAndClient()
        {
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                TaskHubName = "OpenTelemetrySample",
                StorageConnectionString = "UseDevelopmentStorage=true",
            };

            IOrchestrationService service = new AzureStorageOrchestrationService(settings);
            IOrchestrationServiceClient client = (IOrchestrationServiceClient)service;
            await service.CreateIfNotExistsAsync();
            return (service, client);
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
