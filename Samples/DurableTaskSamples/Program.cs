using DurableTask.Settings;

namespace DurableTaskSamples
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using DurableTask;
    using DurableTask.Tracking;
    using DurableTaskSamples.AverageCalculator;
    using DurableTaskSamples.Common.WorkItems;
    using DurableTaskSamples.Cron;
    using DurableTaskSamples.ErrorHandling;
    using DurableTaskSamples.Greetings;
    using DurableTaskSamples.Greetings2;
    using DurableTaskSamples.Replat;
    using DurableTaskSamples.Signal;
    using DurableTaskSamples.SumOfSquares;

    class Program
    {
        static Options options = new Options();

        static void Main(string[] args)
        {
            if (CommandLine.Parser.Default.ParseArgumentsStrict(args, options))
            {
                string servicebusConnectionString = Program.GetSetting("ServiceBusConnectionString");
                string storageConnectionString = Program.GetSetting("StorageConnectionString");
                string taskHubName = ConfigurationManager.AppSettings["taskHubName"];

                IOrchestrationServiceInstanceStore instanceStore = new AzureTableInstanceStore(taskHubName, storageConnectionString);

                ServiceBusOrchestrationService orchestrationServiceAndClient =
                    new ServiceBusOrchestrationService(servicebusConnectionString, taskHubName, instanceStore, null, new ServiceBusOrchestrationServiceSettings());

                TaskHubClient taskHubClient = new TaskHubClient(orchestrationServiceAndClient);
                TaskHubWorker taskHub = new TaskHubWorker(orchestrationServiceAndClient);
                
                if (options.CreateHub)
                {
                    orchestrationServiceAndClient.CreateIfNotExistsAsync().Wait();
                }

                OrchestrationInstance instance = null;

                if (!string.IsNullOrWhiteSpace(options.StartInstance))
                {
                    string instanceId = options.InstanceId ?? Guid.NewGuid().ToString();
                    Console.WriteLine($"Start Orchestration: {options.StartInstance}");
                    switch (options.StartInstance)
                    {
                        case "Greetings":
                            instance = taskHubClient.CreateOrchestrationInstanceAsync(typeof(GreetingsOrchestration), instanceId, null).Result;
                            break;
                        case "Greetings2":
                            if (options.Parameters == null || options.Parameters.Length != 1)
                            {
                                throw new ArgumentException("parameters");
                            }
                            instance = taskHubClient.CreateOrchestrationInstanceAsync(typeof(GreetingsOrchestration2), instanceId, 
                                int.Parse(options.Parameters[0])).Result;
                            break;
                        case "Cron":
                            // Sample Input: "0 12 * */2 Mon"
                            instance = taskHubClient.CreateOrchestrationInstanceAsync(typeof(CronOrchestration), instanceId, 
                                (options.Parameters != null && options.Parameters.Length > 0) ? options.Parameters[0] : null).Result;
                            break;
                        case "Average":
                            // Sample Input: "1 50 10"
                            if (options.Parameters == null || options.Parameters.Length != 3)
                            {
                                throw new ArgumentException("parameters");
                            }
                            int[] input = options.Parameters.Select(p => int.Parse(p)).ToArray();
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
                        case "Replat":
                            instance = taskHubClient.CreateOrchestrationInstanceAsync(typeof(MigrateOrchestration), instanceId,
                                new MigrateOrchestrationData() { SubscriptionId = "03a1cd39-47ac-4a57-9ff5-a2c2a2a76088", IsDisabled = false }).Result;
                            break;
                        default:
                            throw new Exception("Unsupported Orchestration Name: " + options.StartInstance);
                    }

                    Console.WriteLine("Workflow Instance Started: " + instance);
                }
                else if (!string.IsNullOrWhiteSpace(options.Signal))
                {
                    Console.WriteLine("Run RaiseEvent");

                    if (string.IsNullOrWhiteSpace(options.InstanceId)) 
                    {
                        throw new ArgumentException("instanceId");
                    }
                    if (options.Parameters == null || options.Parameters.Length != 1)
                    {
                        throw new ArgumentException("parameters");

                    }
                    string instanceId = options.InstanceId;
                    instance = new OrchestrationInstance { InstanceId = instanceId };
                    taskHubClient.RaiseEventAsync(instance, options.Signal, options.Parameters[0]).Wait();

                    Console.WriteLine("Press any key to quit.");
                    Console.ReadLine();
                }

                if (!options.SkipWorker)
                {
                    try
                    {
                        taskHub.AddTaskOrchestrations(
                            typeof(GreetingsOrchestration),
                            typeof(GreetingsOrchestration2), 
                            typeof(CronOrchestration),
                            typeof(AverageCalculatorOrchestration), 
                            typeof(ErrorHandlingOrchestration), 
                            typeof(SignalOrchestration),
                            typeof(MigrateOrchestration),
                            typeof(SumOfSquaresOrchestration)
                            );

                        taskHub.AddTaskOrchestrations(
                            new NameValueObjectCreator<TaskOrchestration>("SumOfSquaresOrchestration", "V1", typeof(SumOfSquaresOrchestration)));
                        
                        taskHub.AddTaskActivities(
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

                        taskHub.AddTaskActivitiesFromInterface<IManagementSqlOrchestrationTasks>(new ManagementSqlOrchestrationTasks());
                        taskHub.AddTaskActivitiesFromInterface<IMigrationTasks>(new MigrationTasks());

                        taskHub.StartAsync().Wait();

                        Console.WriteLine("Waiting up to 60 seconds for completion.");

                        var taskResult = taskHubClient.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(60), CancellationToken.None).Result;
                        Console.WriteLine($"Task done: {taskResult?.OrchestrationStatus}");

                        Console.WriteLine("Press any key to quit.");
                        Console.ReadLine();

                        taskHub.StopAsync(true).Wait();
                    }
                    catch (Exception e)
                    {
                        // silently eat any unhadled exceptions.
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
            if (string.IsNullOrEmpty(value))
            {
                value = ConfigurationManager.AppSettings.Get(name);
            }
            return value;
        }
    }
}
