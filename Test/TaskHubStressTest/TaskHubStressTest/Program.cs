using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DurableTask;

namespace TaskHubStressTest
{
    class Program
    {
        static Options options = new Options();

        static void Main(string[] args)
        {
            string tableConnectionString = ConfigurationManager.AppSettings["StorageConnectionString"];
            if (CommandLine.Parser.Default.ParseArgumentsStrict(args, options))
            {
                string connectionString = ConfigurationManager.ConnectionStrings["Microsoft.ServiceBus.ConnectionString"].ConnectionString;
                string taskHubName = ConfigurationManager.AppSettings["TaskHubName"];

                TaskHubClient taskHubClient = new TaskHubClient(taskHubName, connectionString, tableConnectionString);
                TaskHubWorkerSettings settings = new TaskHubWorkerSettings();
                settings.TaskOrchestrationDispatcherSettings.CompressOrchestrationState = bool.Parse(ConfigurationManager.AppSettings["CompressOrchestrationState"]);
                settings.TaskActivityDispatcherSettings.MaxConcurrentActivities = int.Parse(ConfigurationManager.AppSettings["MaxConcurrentActivities"]);
                settings.TaskOrchestrationDispatcherSettings.MaxConcurrentOrchestrations = int.Parse(ConfigurationManager.AppSettings["MaxConcurrentOrchestrations"]);
                TaskHubWorker taskHub = new TaskHubWorker(taskHubName, connectionString, tableConnectionString, settings);

                if (options.CreateHub)
                {
                    taskHub.CreateHub();
                }

                OrchestrationInstance instance = null;
                string instanceId = options.StartInstance;

                if (!string.IsNullOrWhiteSpace(instanceId))
                {
                    instance = taskHubClient.CreateOrchestrationInstance(typeof(DriverOrchestration), instanceId, new DriverOrchestrationData
                    {
                        NumberOfIteration = int.Parse(ConfigurationManager.AppSettings["DriverOrchestrationIterations"]),
                        NumberOfParallelTasks = int.Parse(ConfigurationManager.AppSettings["DriverOrchestrationParallelTasks"]),
                        SubOrchestrationData = new TestOrchestrationData
                        {
                            NumberOfParallelTasks = int.Parse(ConfigurationManager.AppSettings["ChildOrchestrationParallelTasks"]),
                            NumberOfSerialTasks = int.Parse(ConfigurationManager.AppSettings["ChildOrchestrationSerialTasks"]),
                            MaxDelayInSeconds = int.Parse(ConfigurationManager.AppSettings["TestTaskMaxDelayInMinutes"]),
                        },
                    });
                }
                else
                {
                    instance = new OrchestrationInstance { InstanceId = options.InstanceId };
                }

                Stopwatch stopWatch = new Stopwatch();
                stopWatch.Start();

                TestTask testTask = new TestTask();
                taskHub.AddTaskActivities(testTask);
                taskHub.AddTaskOrchestrations(typeof(DriverOrchestration));
                taskHub.AddTaskOrchestrations(typeof(TestOrchestration));
                taskHub.Start();

                int testTimeoutInSeconds = int.Parse(ConfigurationManager.AppSettings["TestTimeoutInSeconds"]);
                OrchestrationState state = WaitForInstance(taskHubClient, instance, testTimeoutInSeconds);
                stopWatch.Stop();
                Console.WriteLine("Orchestration Status: " + state.OrchestrationStatus.ToString());
                Console.WriteLine("Orchestration Result: " + state.Output);
                Console.WriteLine("Counter: " + testTask.counter);

                TimeSpan totalTime = stopWatch.Elapsed;
                string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                    totalTime.Hours, totalTime.Minutes, totalTime.Seconds,
                    totalTime.Milliseconds / 10);
                Console.WriteLine("Total Time: " + elapsedTime);

                taskHub.Stop();
            }

        }

        public static OrchestrationState WaitForInstance(TaskHubClient taskHubClient, OrchestrationInstance instance, int timeoutSeconds)
        {
            OrchestrationStatus status = OrchestrationStatus.Running;
            if (instance == null || string.IsNullOrWhiteSpace(instance.InstanceId))
            {
                throw new ArgumentException("instance");
            }

            int sleepForSeconds = 30;
            while (timeoutSeconds > 0)
            {
                try
                {
                    var state = taskHubClient.GetOrchestrationState(instance.InstanceId);
                    if (state != null) status = state.OrchestrationStatus;
                    if (status == OrchestrationStatus.Running)
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
                    Console.WriteLine(string.Format("Error retrieving state for instance [instanceId: '{0}', executionId: '{1}'].", instance.InstanceId, instance.ExecutionId));
                    Console.WriteLine(ex.ToString());
                }
            }

            throw new TimeoutException("Timeout expired: " + timeoutSeconds.ToString());
        }
    }
}
