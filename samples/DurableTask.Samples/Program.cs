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

namespace DurableTask.Samples
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Diagnostics.Tracing;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using DurableTask.AzureStorage;
    using DurableTask.Core;
    using DurableTask.Core.Tracing;
    using DurableTask.Samples.AverageCalculator;
    using DurableTask.Samples.Common.WorkItems;
    using DurableTask.Samples.Cron;
    using DurableTask.Samples.ErrorHandling;
    using DurableTask.Samples.Greetings;
    using DurableTask.Samples.Greetings2;
    using DurableTask.Samples.Replat;
    using DurableTask.Samples.Signal;
    using DurableTask.Samples.SumOfSquares;
    using Microsoft.Practices.EnterpriseLibrary.SemanticLogging;

    internal class Program
    {
        static readonly Options ArgumentOptions = new Options();
        static ObservableEventListener eventListener;

        [STAThread]
        static void Main(string[] args)
        {
            eventListener = new ObservableEventListener();
            eventListener.LogToConsole();
            eventListener.EnableEvents(DefaultEventSource.Log, EventLevel.LogAlways);

            if (CommandLine.Parser.Default.ParseArgumentsStrict(args, ArgumentOptions))
            {
                string storageConnectionString = GetSetting("StorageConnectionString");
                string taskHubName = ConfigurationManager.AppSettings["taskHubName"];

                var settings = new AzureStorageOrchestrationServiceSettings
                {
                    StorageAccountClientProvider = new StorageAccountClientProvider(storageConnectionString),
                    TaskHubName = taskHubName,
                };

                var orchestrationServiceAndClient = new AzureStorageOrchestrationService(settings);
                var taskHubClient = new TaskHubClient(orchestrationServiceAndClient);
                var taskHubWorker = new TaskHubWorker(orchestrationServiceAndClient);
                
                if (ArgumentOptions.CreateHub)
                {
                    orchestrationServiceAndClient.CreateIfNotExistsAsync().Wait();
                }

                OrchestrationInstance instance = null;

                if (!string.IsNullOrWhiteSpace(ArgumentOptions.StartInstance))
                {
                    string instanceId = ArgumentOptions.InstanceId ?? Guid.NewGuid().ToString();
                    Console.WriteLine($"Start Orchestration: {ArgumentOptions.StartInstance}");
                    switch (ArgumentOptions.StartInstance)
                    {
                        case "Greetings":
                            instance = taskHubClient.CreateOrchestrationInstanceAsync(typeof(GreetingsOrchestration), instanceId, null).Result;
                            break;
                        case "Greetings2":
                            if (ArgumentOptions.Parameters == null || ArgumentOptions.Parameters.Length != 1)
                            {
                                throw new ArgumentException("parameters");
                            }

                            instance = taskHubClient.CreateOrchestrationInstanceAsync(typeof(GreetingsOrchestration2), instanceId, 
                                int.Parse(ArgumentOptions.Parameters[0])).Result;
                            break;
                        case "Cron":
                            // Sample Input: "0 12 * */2 Mon"
                            instance = taskHubClient.CreateOrchestrationInstanceAsync(typeof(CronOrchestration), instanceId, 
                                (ArgumentOptions.Parameters != null && ArgumentOptions.Parameters.Length > 0) ? ArgumentOptions.Parameters[0] : null).Result;
                            break;
                        case "Average":
                            // Sample Input: "1 50 10"
                            if (ArgumentOptions.Parameters == null || ArgumentOptions.Parameters.Length != 3)
                            {
                                throw new ArgumentException("parameters");
                            }

                            int[] input = ArgumentOptions.Parameters.Select(p => int.Parse(p)).ToArray();
                            instance = taskHubClient.CreateOrchestrationInstanceAsync(typeof(AverageCalculatorOrchestration), instanceId, input).Result;
                            break;
                        case "ErrorHandling":
                            instance = taskHubClient.CreateOrchestrationInstanceAsync(typeof(ErrorHandlingOrchestration), instanceId, null).Result;
                            break;
                        case "SumOfSquares":
                            instance = taskHubClient.CreateOrchestrationInstanceAsync(
                                "SumOfSquaresOrchestration", 
                                "V1", 
                                instanceId, 
                                File.ReadAllText("SumofSquares\\BagOfNumbers.json"),
                                new Dictionary<string, string>(1) { { "Category", "testing" } }).Result;
                            break;
                        case "Signal":
                            instance = taskHubClient.CreateOrchestrationInstanceAsync(typeof(SignalOrchestration), instanceId, null).Result;
                            break;
                        case "SignalAndRaise":
                            if (ArgumentOptions.Parameters == null || ArgumentOptions.Parameters.Length != 1)
                            {
                                throw new ArgumentException("parameters");
                            }

                            instance = taskHubClient.CreateOrchestrationInstanceWithRaisedEventAsync(typeof(SignalOrchestration), instanceId, null, ArgumentOptions.Signal, ArgumentOptions.Parameters[0]).Result;
                            break;
                        case "Replat":
                            instance = taskHubClient.CreateOrchestrationInstanceAsync(typeof(MigrateOrchestration), instanceId,
                                new MigrateOrchestrationData { SubscriptionId = "03a1cd39-47ac-4a57-9ff5-a2c2a2a76088", IsDisabled = false }).Result;
                            break;
                        default:
                            throw new Exception("Unsupported Orchestration Name: " + ArgumentOptions.StartInstance);
                    }

                    Console.WriteLine("Workflow Instance Started: " + instance);
                }
                else if (!string.IsNullOrWhiteSpace(ArgumentOptions.Signal))
                {
                    Console.WriteLine("Run RaiseEvent");

                    if (string.IsNullOrWhiteSpace(ArgumentOptions.InstanceId)) 
                    {
                        throw new ArgumentException("instanceId");
                    }

                    if (ArgumentOptions.Parameters == null || ArgumentOptions.Parameters.Length != 1)
                    {
                        throw new ArgumentException("parameters");
                    }

                    string instanceId = ArgumentOptions.InstanceId;
                    instance = new OrchestrationInstance { InstanceId = instanceId };
                    taskHubClient.RaiseEventAsync(instance, ArgumentOptions.Signal, ArgumentOptions.Parameters[0]).Wait();

                    Console.WriteLine("Press any key to quit.");
                    Console.ReadLine();
                }

                if (!ArgumentOptions.SkipWorker)
                {
                    try
                    {
                        taskHubWorker.AddTaskOrchestrations(
                            typeof(GreetingsOrchestration),
                            typeof(GreetingsOrchestration2), 
                            typeof(CronOrchestration),
                            typeof(AverageCalculatorOrchestration), 
                            typeof(ErrorHandlingOrchestration), 
                            typeof(SignalOrchestration),
                            typeof(MigrateOrchestration),
                            typeof(SumOfSquaresOrchestration)
                            );

                        taskHubWorker.AddTaskOrchestrations(
                            new NameValueObjectCreator<TaskOrchestration>("SumOfSquaresOrchestration", "V1", typeof(SumOfSquaresOrchestration)));
                        
                        taskHubWorker.AddTaskActivities(
                            new GetUserTask(), 
                            new SendGreetingTask(), 
                            new CronTask(), 
                            new ComputeSumTask(), 
                            new GoodTask(), 
                            new BadTask(), 
                            new CleanupTask(),
                            new EmailTask(),
                            new SumOfSquaresTask()
                            );

                        taskHubWorker.AddTaskActivitiesFromInterface<IManagementSqlOrchestrationTasks>(new ManagementSqlOrchestrationTasks());
                        taskHubWorker.AddTaskActivitiesFromInterface<IMigrationTasks>(new MigrationTasks());

                        taskHubWorker.StartAsync().Wait();

                        Console.WriteLine("Waiting up to 60 seconds for completion.");

                        OrchestrationState taskResult = taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(60), CancellationToken.None).Result;
                        Console.WriteLine($"Task done: {taskResult?.OrchestrationStatus}");

                        Console.WriteLine("Press any key to quit.");
                        Console.ReadLine();

                        taskHubWorker.StopAsync(true).Wait();
                    }
                    catch (Exception e)
                    {
                        // silently eat any unhandled exceptions.
                        Console.WriteLine($"worker exception: {e}");
                    }
                }
                else
                {
                    Console.WriteLine("Skip Worker");
                }
            }
        }

        public static string GetSetting(string name)
        {
            string value = Environment.GetEnvironmentVariable("DurableTaskTest" + name);
            if (string.IsNullOrWhiteSpace(value))
            {
                value = ConfigurationManager.AppSettings.Get(name);
            }

            return value;
        }
    }
}
