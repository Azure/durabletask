namespace DurableTaskSamples
{
    using System;
    using System.Configuration;
    using System.Linq;
    using DurableTask;
    using DurableTaskSamples.AverageCalculator;
    using DurableTaskSamples.Common.WorkItems;
    using DurableTaskSamples.Cron;
    using DurableTaskSamples.ErrorHandling;
    using DurableTaskSamples.Greetings;
    using DurableTaskSamples.Greetings2;
    using DurableTaskSamples.Replat;
    using DurableTaskSamples.Signal;

    class Program
    {
        static Options options = new Options();

        static void Main(string[] args)
        {
            if (args.Length == 0)
                args = new string[1]{ "-?"};
                

            if (CommandLine.Parser.Default.ParseArgumentsStrict(args, options))
            {
                string servicebusConnectionString = Program.GetSetting("ServiceBusConnectionString");
                string storageConnectionString = Program.GetSetting("StorageConnectionString");
                string taskHubName = ConfigurationManager.AppSettings["taskHubName"];

                TaskHubClient taskHubClient = new TaskHubClient(taskHubName, servicebusConnectionString, storageConnectionString);
                TaskHubWorker taskHub = new TaskHubWorker(taskHubName, servicebusConnectionString, storageConnectionString);

                if (options.CreateHub)
                {
                    taskHub.CreateHub();
                }

                if (!string.IsNullOrWhiteSpace(options.StartInstance))
                {
                    string instanceId = options.InstanceId;
                    OrchestrationInstance instance = null;
                    switch (options.StartInstance)
                    {
                        case "Greetings":
                            instance = taskHubClient.CreateOrchestrationInstance(typeof(GreetingsOrchestration), instanceId, null);
                            break;
                        case "Greetings2":
                            if (options.Parameters == null || options.Parameters.Length != 1)
                            {
                                throw new ArgumentException("parameters");
                            }
                            instance = taskHubClient.CreateOrchestrationInstance(typeof(GreetingsOrchestration2), instanceId, 
                                int.Parse(options.Parameters[0]));
                            break;
                        case "Cron":
                            // Sample Input: "0 12 * */2 Mon"
                            instance = taskHubClient.CreateOrchestrationInstance(typeof(CronOrchestration), instanceId, 
                                (options.Parameters != null && options.Parameters.Length > 0) ? options.Parameters[0] : null);
                            break;
                        case "Average":
                            // Sample Input: "1 50 10"
                            if (options.Parameters == null || options.Parameters.Length != 3)
                            {
                                throw new ArgumentException("parameters");
                            }
                            int[] input = options.Parameters.Select(p => int.Parse(p)).ToArray();
                            instance = taskHubClient.CreateOrchestrationInstance(typeof(AverageCalculatorOrchestration), instanceId, input);
                            break;
                        case "ErrorHandling":
                            instance = taskHubClient.CreateOrchestrationInstance(typeof(ErrorHandlingOrchestration), instanceId, null);
                            break;
                        case "Signal":
                            instance = taskHubClient.CreateOrchestrationInstance(typeof(SignalOrchestration), instanceId, null);
                            break;
                        case "Replat":
                            instance = taskHubClient.CreateOrchestrationInstance(typeof(MigrateOrchestration), instanceId,
                                new MigrateOrchestrationData() { SubscriptionId = "03a1cd39-47ac-4a57-9ff5-a2c2a2a76088", IsDisabled = false });
                            break;
                        default:
                            throw new Exception("Unsupported Orchestration Name: " + options.StartInstance);
                    }

                    Console.WriteLine("Workflow Instance Started: " + instance);
                }
                else if (!string.IsNullOrWhiteSpace(options.Signal))
                {
                    if (string.IsNullOrWhiteSpace(options.InstanceId)) 
                    {
                        throw new ArgumentException("instantceId");
                    }
                    if (options.Parameters == null || options.Parameters.Length != 1)
                    {
                        throw new ArgumentException("parameters");

                    }
                    string instanceId = options.InstanceId;
                    OrchestrationInstance instance = new OrchestrationInstance { InstanceId = instanceId };
                    taskHubClient.RaiseEvent(instance, options.Signal, options.Parameters[0]);
                }

                if (!options.SkipWorker)
                {
                    try
                    {
                        taskHub.AddTaskOrchestrations(typeof(GreetingsOrchestration), typeof(GreetingsOrchestration2), typeof(CronOrchestration),
                                                            typeof (AverageCalculatorOrchestration), typeof (ErrorHandlingOrchestration), typeof (SignalOrchestration));
                        taskHub.AddTaskOrchestrations(typeof(MigrateOrchestration));

                        taskHub.AddTaskActivities(new GetUserTask(), new SendGreetingTask(), new CronTask(), new ComputeSumTask(), new GoodTask(), new BadTask(), new CleanupTask(),
                                                             new EmailTask());
                        taskHub.AddTaskActivitiesFromInterface<IManagementSqlOrchestrationTasks>(new ManagementSqlOrchestrationTasks());
                        taskHub.AddTaskActivitiesFromInterface<IMigrationTasks>(new MigrationTasks());

                        taskHub.Start();

                        Console.WriteLine("Press any key to quit.");
                        Console.ReadLine();

                        taskHub.Stop(true);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex);
                    }
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
