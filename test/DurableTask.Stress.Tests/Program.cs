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

#if NETCOREAPP
[assembly: System.Runtime.InteropServices.ComVisible(false)]

namespace DurableTask.Stress.Tests
{
    using System;
    using System.Configuration;
    using System.Diagnostics;
    using CommandLine;
    using DurableTask.AzureStorage;
    using DurableTask.Core;
    using DurableTask.Test.Orchestrations.Stress;
    using Microsoft.Diagnostics.EventFlow;

    internal class Program
    {
        // ReSharper disable once UnusedMember.Local
        static void Main(string[] args)
        {
            using (DiagnosticPipelineFactory.CreatePipeline("eventFlowConfig.json"))
            {
                var config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
                ParserResult<Options> parserResult = Parser.Default.ParseArguments<Options>(args);
                parserResult.WithParsed(
                        options =>
                        {
                            string connectionString = config.ConnectionStrings.ConnectionStrings["AzureStorage"].ConnectionString;
                            var settings = new AzureStorageOrchestrationServiceSettings
                            {
                                MaxConcurrentTaskActivityWorkItems = int.Parse(config.AppSettings.Settings["MaxConcurrentActivities"].Value),
                                MaxConcurrentTaskOrchestrationWorkItems = int.Parse(config.AppSettings.Settings["MaxConcurrentOrchestrations"].Value),
                                StorageAccountClientProvider = new StorageAccountClientProvider(connectionString),
                                TaskHubName = config.AppSettings.Settings["TaskHubName"].Value,
                            };

                            var orchestrationServiceAndClient = new AzureStorageOrchestrationService(settings);
                            var taskHubClient = new TaskHubClient(orchestrationServiceAndClient);
                            var taskHub = new TaskHubWorker(orchestrationServiceAndClient);

                            if (options.CreateHub)
                            {
                                orchestrationServiceAndClient.CreateIfNotExistsAsync().Wait();
                            }

                            OrchestrationInstance instance;
                            string instanceId = options.StartInstance;

                            if (!string.IsNullOrWhiteSpace(instanceId))
                            {
                                var driverConfig = new DriverOrchestrationData
                                {
                                    NumberOfIteration = int.Parse(config.AppSettings.Settings["DriverOrchestrationIterations"].Value),
                                    NumberOfParallelTasks = int.Parse(config.AppSettings.Settings["DriverOrchestrationParallelTasks"].Value),
                                    SubOrchestrationData = new TestOrchestrationData
                                    {
                                        NumberOfParallelTasks = int.Parse(config.AppSettings.Settings["ChildOrchestrationParallelTasks"].Value),
                                        NumberOfSerialTasks = int.Parse(config.AppSettings.Settings["ChildOrchestrationSerialTasks"].Value),
                                        MaxDelayInMinutes = int.Parse(config.AppSettings.Settings["TestTaskMaxDelayInMinutes"].Value),
                                    }
                                };

                                instance = taskHubClient.CreateOrchestrationInstanceAsync(typeof(DriverOrchestration), instanceId, driverConfig).Result;
                            }
                            else
                            {
                                instance = new OrchestrationInstance { InstanceId = options.InstanceId };
                            }

                            Console.WriteLine($"Orchestration starting: {DateTime.Now}");
                            Stopwatch stopWatch = Stopwatch.StartNew();

                            var testTask = new TestTask();
                            taskHub.AddTaskActivities(testTask);
                            taskHub.AddTaskOrchestrations(typeof(DriverOrchestration));
                            taskHub.AddTaskOrchestrations(typeof(TestOrchestration));
                            taskHub.StartAsync().Wait();

                            int testTimeoutInSeconds = int.Parse(config.AppSettings.Settings["TestTimeoutInSeconds"].Value);
                            OrchestrationState state = WaitForInstance(taskHubClient, instance, testTimeoutInSeconds);
                            stopWatch.Stop();
                            Console.WriteLine($"Orchestration Status: {state.OrchestrationStatus}");
                            Console.WriteLine($"Orchestration Result: {state.Output}");
                            Console.WriteLine($"Counter: {testTask.Counter}");

                            TimeSpan totalTime = stopWatch.Elapsed;
                            string elapsedTime = $"{totalTime.Hours:00}:{totalTime.Minutes:00}:{totalTime.Seconds:00}.{totalTime.Milliseconds / 10:00}";
                            Console.WriteLine($"Total Time: {elapsedTime}");
                            Console.ReadLine();

                            taskHub.StopAsync().Wait();
                        })
                    .WithNotParsed(errors => Console.Error.WriteLine(Options.GetUsage(parserResult)));
            }
        }

        public static OrchestrationState WaitForInstance(TaskHubClient taskHubClient, OrchestrationInstance instance, int timeoutSeconds)
        {
            var status = OrchestrationStatus.Running;
            if (string.IsNullOrWhiteSpace(instance?.InstanceId))
            {
                throw new ArgumentException("instance");
            }

            var sleepForSeconds = 30;
            while (timeoutSeconds > 0)
            {
                try
                {
                    OrchestrationState state = taskHubClient.GetOrchestrationStateAsync(instance.InstanceId).Result;
                    if (state != null) status = state.OrchestrationStatus;
                    if (status == OrchestrationStatus.Running || status == OrchestrationStatus.Pending)
                    {
                        System.Threading.Thread.Sleep(sleepForSeconds * 1000);
                        timeoutSeconds -= sleepForSeconds;
                    }
                    else
                    {
                        // Session state deleted after completion
                        return state;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error retrieving state for instance [instanceId: '{instance.InstanceId}', executionId: '{instance.ExecutionId}'].");
                    Console.WriteLine(ex.ToString());
                }
            }

            throw new TimeoutException("Timeout expired: " + timeoutSeconds.ToString());
        }
    }
}
#else
namespace DurableTask.Stress.Tests
{
    using System;
    using System.Configuration;
    using System.Diagnostics;
    using System.Diagnostics.Tracing;
    using DurableTask.AzureStorage;
    using DurableTask.Core;
    using DurableTask.Core.Tracing;
    using DurableTask.Test.Orchestrations.Stress;
    using Microsoft.Practices.EnterpriseLibrary.SemanticLogging;

    internal class Program
    {
        static readonly Options ArgumentOptions = new Options();
        static ObservableEventListener eventListener;

        // ReSharper disable once UnusedMember.Local
        static void Main(string[] args)
        {
            eventListener = new ObservableEventListener();
            eventListener.LogToFlatFile("Trace.log");
            eventListener.EnableEvents(DefaultEventSource.Log, EventLevel.Warning);

            string tableConnectionString = ConfigurationManager.AppSettings["StorageConnectionString"];

            if (CommandLine.Parser.Default.ParseArgumentsStrict(args, ArgumentOptions))
            {
                string connectionString = ConfigurationManager.ConnectionStrings["AzureStorage"].ConnectionString;
                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    MaxConcurrentTaskActivityWorkItems = int.Parse(ConfigurationManager.AppSettings["MaxConcurrentActivities"]),
                    MaxConcurrentTaskOrchestrationWorkItems = int.Parse(ConfigurationManager.AppSettings["MaxConcurrentOrchestrations"]),
                    StorageAccountClientProvider = new StorageAccountClientProvider(connectionString),
                    TaskHubName = ConfigurationManager.AppSettings["TaskHubName"],
                };

                var orchestrationServiceAndClient = new AzureStorageOrchestrationService(settings);

                var taskHubClient = new TaskHubClient(orchestrationServiceAndClient);
                var taskHub = new TaskHubWorker(orchestrationServiceAndClient);

                if (ArgumentOptions.CreateHub)
                {
                    orchestrationServiceAndClient.CreateIfNotExistsAsync().Wait();
                }

                OrchestrationInstance instance;
                string instanceId = ArgumentOptions.StartInstance;

                if (!string.IsNullOrWhiteSpace(instanceId))
                {
                    var driverConfig = new DriverOrchestrationData
                    {
                        NumberOfIteration = int.Parse(ConfigurationManager.AppSettings["DriverOrchestrationIterations"]),
                        NumberOfParallelTasks = int.Parse(ConfigurationManager.AppSettings["DriverOrchestrationParallelTasks"]),
                        SubOrchestrationData = new TestOrchestrationData
                        {
                            NumberOfParallelTasks = int.Parse(ConfigurationManager.AppSettings["ChildOrchestrationParallelTasks"]),
                            NumberOfSerialTasks = int.Parse(ConfigurationManager.AppSettings["ChildOrchestrationSerialTasks"]),
                            MaxDelayInMinutes = int.Parse(ConfigurationManager.AppSettings["TestTaskMaxDelayInMinutes"]),
                        }
                    };

                    instance = taskHubClient.CreateOrchestrationInstanceAsync(typeof(DriverOrchestration), instanceId, driverConfig).Result;
                }
                else
                {
                    instance = new OrchestrationInstance { InstanceId = ArgumentOptions.InstanceId };
                }

                Console.WriteLine($"Orchestration starting: {DateTime.Now}");
                Stopwatch stopWatch = Stopwatch.StartNew();

                var testTask = new TestTask();
                taskHub.AddTaskActivities(testTask);
                taskHub.AddTaskOrchestrations(typeof(DriverOrchestration));
                taskHub.AddTaskOrchestrations(typeof(TestOrchestration));
                taskHub.StartAsync().Wait();

                int testTimeoutInSeconds = int.Parse(ConfigurationManager.AppSettings["TestTimeoutInSeconds"]);
                OrchestrationState state = WaitForInstance(taskHubClient, instance, testTimeoutInSeconds);
                stopWatch.Stop();
                Console.WriteLine($"Orchestration Status: {state.OrchestrationStatus}");
                Console.WriteLine($"Orchestration Result: {state.Output}");
                Console.WriteLine($"Counter: {testTask.Counter}");

                TimeSpan totalTime = stopWatch.Elapsed;
                string elapsedTime = $"{totalTime.Hours:00}:{totalTime.Minutes:00}:{totalTime.Seconds:00}.{totalTime.Milliseconds / 10:00}";
                Console.WriteLine($"Total Time: {elapsedTime}");
                Console.ReadLine();

                taskHub.StopAsync().Wait();
            }
        }

        public static OrchestrationState WaitForInstance(TaskHubClient taskHubClient, OrchestrationInstance instance, int timeoutSeconds)
        {
            var status = OrchestrationStatus.Running;
            if (string.IsNullOrWhiteSpace(instance?.InstanceId))
            {
                throw new ArgumentException("instance");
            }

            var sleepForSeconds = 30;
            while (timeoutSeconds > 0)
            {
                try
                {
                    OrchestrationState state = taskHubClient.GetOrchestrationStateAsync(instance.InstanceId).Result;
                    if (state != null) status = state.OrchestrationStatus;
                    if (status == OrchestrationStatus.Running || status == OrchestrationStatus.Pending)
                    {
                        System.Threading.Thread.Sleep(sleepForSeconds * 1000);
                        timeoutSeconds -= sleepForSeconds;
                    }
                    else
                    {
                        // Session state deleted after completion
                        return state;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error retrieving state for instance [instanceId: '{instance.InstanceId}', executionId: '{instance.ExecutionId}'].");
                    Console.WriteLine(ex.ToString());
                }
            }

            throw new TimeoutException("Timeout expired: " + timeoutSeconds.ToString());
        }
    }
}

#endif