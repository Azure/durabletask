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

namespace ApplicationInsightsSample
{
    using System;
    using System.Threading.Tasks;
    using DurableTask.ApplicationInsights;
    using DurableTask.AzureStorage;
    using DurableTask.Core;
    using DurableTask.DependencyInjection;
    using DurableTask.Hosting;
    using Microsoft.ApplicationInsights.DependencyCollector;
    using Microsoft.ApplicationInsights.Extensibility;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.DependencyInjection.Extensions;
    using Microsoft.Extensions.Hosting;

    internal class Program
    {
        static async Task Main(string[] args)
        {
            IHost host = Host.CreateDefaultBuilder(args)
                .ConfigureServices(services =>
                {
                    services.TryAddEnumerable(ServiceDescriptor.Singleton<ITelemetryModule, DurableTelemetryModule>());
                    services.AddSingleton(GetOrchestrationService());
                    AddApplicationInsights(services);
                })
                .ConfigureTaskHubWorker(builder =>
                {
                    builder.AddClient();

                    builder.AddActivitiesFromAssembly<Program>(includePrivate: true);
                    builder.AddOrchestrationsFromAssembly<Program>(includePrivate: true);
                })
                .Build();

            await host.StartAsync();
            TaskHubClient client = host.Services.GetRequiredService<TaskHubClient>();
            OrchestrationInstance orchestrationInstance = await client.CreateOrchestrationInstanceAsync(
                typeof(Orchestration), input: null);
            await client.WaitForOrchestrationAsync(orchestrationInstance, TimeSpan.FromMinutes(5));

            await host.StopAsync();
            host.Dispose();
            await Task.Delay(10000); // wait 10s for flush
        }

        static IOrchestrationService GetOrchestrationService()
        {
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                TaskHubName = "AppInsightsSample",
                StorageAccountClientProvider = new StorageAccountClientProvider("UseDevelopmentStorage=true"),
            };
            IOrchestrationService service = new AzureStorageOrchestrationService(settings);
            return service;
        }

        static void AddApplicationInsights(IServiceCollection services)
        {
            services.AddApplicationInsightsTelemetryWorkerService();
            services.ConfigureTelemetryModule<DependencyTrackingTelemetryModule>((m, _) =>
            {
                // Disabling Request-Ids in headers since we're using W3C protocol
                m.EnableRequestIdHeaderInjectionInW3CMode = false;
            });

            // Used for demo purposes only as this will filter out many spans.
            services.AddApplicationInsightsTelemetryProcessor<FilterOutStorageTelemetryProcessor>();
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

        class SayHello : AsyncTaskActivity<string, string>
        {
            protected override async Task<string> ExecuteAsync(TaskContext context, string input)
            {
                await Task.Delay(100);
                return $"Hello, {input}!";
            }
        }
    }
}
