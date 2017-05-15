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

[assembly: System.Runtime.InteropServices.ComVisible(false)]
namespace DurableTask.Stress.Tests
{
    using System;
    using System.Configuration;
    using System.Diagnostics;
    using DurableTask.Core;
    using DurableTask.Core.Tracing;
    using DurableTask.ServiceBus.Settings;
    using DurableTask.ServiceBus;
    using DurableTask.Test.Orchestrations.Stress;
    using DurableTask.ServiceBus.Tracking;
    using Microsoft.Practices.EnterpriseLibrary.SemanticLogging;
    using System.Diagnostics.Tracing;

    class Program
    {
        static Options options = new Options();
        static ObservableEventListener eventListener;

        static void Main(string[] args)
        {
            eventListener = new ObservableEventListener();
            eventListener.LogToFlatFile("Trace.log");
            eventListener.EnableEvents(DefaultEventSource.Log, EventLevel.Warning);

            string tableConnectionString = ConfigurationManager.AppSettings["StorageConnectionString"];

            if (CommandLine.Parser.Default.ParseArgumentsStrict(args, options))
            {
                string connectionString = ConfigurationManager.ConnectionStrings["Microsoft.ServiceBus.ConnectionString"].ConnectionString;
                string taskHubName = ConfigurationManager.AppSettings["TaskHubName"];
           

                IOrchestrationServiceInstanceStore instanceStore = new AzureTableInstanceStore(taskHubName, tableConnectionString);

                ServiceBusOrchestrationServiceSettings settings = new ServiceBusOrchestrationServiceSettings
                {
                    TaskOrchestrationDispatcherSettings =
                    {
                        CompressOrchestrationState = bool.Parse(ConfigurationManager.AppSettings["CompressOrchestrationState"]),
                        MaxConcurrentOrchestrations = int.Parse(ConfigurationManager.AppSettings["MaxConcurrentOrchestrations"])
                    },
                    TaskActivityDispatcherSettings =
                    {
                        MaxConcurrentActivities = int.Parse(ConfigurationManager.AppSettings["MaxConcurrentActivities"])
                    }
                };

                ServiceBusOrchestrationService orchestrationServiceAndClient =
                    new ServiceBusOrchestrationService(connectionString, taskHubName, instanceStore, null, settings);


                TaskHubClient taskHubClient = new TaskHubClient(orchestrationServiceAndClient);
                TaskHubWorker taskHub = new TaskHubWorker(orchestrationServiceAndClient);

                if (options.CreateHub)
                {
                    orchestrationServiceAndClient.CreateIfNotExistsAsync().Wait();
                }

                OrchestrationInstance instance = null;
                string instanceId = options.StartInstance;

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
                    instance = new OrchestrationInstance { InstanceId = options.InstanceId };
                }

                Console.WriteLine($"Orchestration starting: {DateTime.Now}");
                Stopwatch stopWatch = Stopwatch.StartNew();

                TestTask testTask = new TestTask();
                taskHub.AddTaskActivities(testTask);
                taskHub.AddTaskOrchestrations(typeof(DriverOrchestration));
                taskHub.AddTaskOrchestrations(typeof(TestOrchestration));
                taskHub.StartAsync().Wait();

                int testTimeoutInSeconds = int.Parse(ConfigurationManager.AppSettings["TestTimeoutInSeconds"]);
                OrchestrationState state = WaitForInstance(taskHubClient, instance, testTimeoutInSeconds);
                stopWatch.Stop();
                Console.WriteLine($"Orchestration Status: {state.OrchestrationStatus}");
                Console.WriteLine($"Orchestration Result: {state.Output}");
                Console.WriteLine($"Counter: {testTask.counter}");

                TimeSpan totalTime = stopWatch.Elapsed;
                string elapsedTime = $"{totalTime.Hours:00}:{totalTime.Minutes:00}:{totalTime.Seconds:00}.{totalTime.Milliseconds/10:00}";
                Console.WriteLine($"Total Time: {elapsedTime}");
                Console.ReadLine();

                taskHub.StopAsync().Wait();
            }

        }

        public static OrchestrationState WaitForInstance(TaskHubClient taskHubClient, OrchestrationInstance instance, int timeoutSeconds)
        {
            OrchestrationStatus status = OrchestrationStatus.Running;
            if (string.IsNullOrWhiteSpace(instance?.InstanceId))
            {
                throw new ArgumentException("instance");
            }

            int sleepForSeconds = 30;
            while (timeoutSeconds > 0)
            {
                try
                {
                    var state = taskHubClient.GetOrchestrationStateAsync(instance.InstanceId).Result;
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
