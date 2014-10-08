using ServiceBusTaskScheduler.Async;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceBusTaskScheduler
{
//    public class TaskWorker2
//    {
//        IDictionary<string, TaskImplementation> taskImplementations;
//        LimitedConcurrencyLevelTaskScheduler taskScheduler;
//        private bool isShuttingDown;
//
//        public TaskWorker2(string tasklist)
//        {
//            this.Tasklist = tasklist;
//            this.PollThreadCount = 5;
//            this.taskImplementations = new Dictionary<string, TaskImplementation>();
//        }
//
//        class PollServiceBusTask
//        {
//            string workerName;
//            TaskWorker worker;
//
//            public PollServiceBusTask(string workerName, TaskWorker worker)
//            {
//                this.workerName = workerName;
//                this.worker = worker;
//            }
//
//            public void Start()
//            {
//                //Thread.CurrentThread.Name = this.workerName;
//
//                try
//                {
//                    Console.WriteLine("Starting Service Bus Poll for worker: " + this.workerName);
//                    this.PollAndProcessSingleTask();
//
//                }
//                catch (Exception ex)
//                {
//                    // Exception should never bubble here.  
//                    // Log the exception and keep going
//                    string error = string.Format("Exception in worker: {0}, Exception: {1}, CallStack: {2}", this.workerName, ex.ToString(), ex.StackTrace);
//                    Console.WriteLine(error);
//                }
//                finally
//                {
//                    if (!worker.isShuttingDown)
//                    {
//                        // Run it again
//                        this.worker.PollForTask(Start);
//                    }
//                }
//            }
//
//            private bool PollAndProcessSingleTask()
//            {
//                Console.WriteLine("Begin Task Poll Request... ");
//                Thread.Sleep(2 * 1000);
//
//                // TODO: Poll ServiceBus for any Activity Task for this Tasklist
//                //ActivityTask activityTask = poll();
//                //if (activityTask != null)
//                //{
//                //    ExecuteTask(activityTask);
//
//                // TODO: Go through each decision and take appropriate action on ServiceBus.
//                // Also update history after taking all those actions
//                //}
//
//                return true;
//            }
//
//            private void ExecuteTask(ActivityTask activityTask)
//            {
//                string key = this.worker.GetKey(activityTask.Name, activityTask.Version);
//                TaskImplementation implementation = this.worker.taskImplementations[key];
//                if (implementation != null)
//                {
//                    TaskContext context = new TaskContext();
//                    string result = implementation.Execute(context, activityTask.Input);
//
//                    // TODO: Now get this result and post a message to Decider queue with Activity Task Completed
//                }
//            }
//        }
//
//        public string Tasklist { get; private set; }
//        public int PollThreadCount { get; set; }
//        public int ExecutionThreadPoolSize { get; set; }
//
//        public void AddTaskImplementation(TaskImplementation taskImplementation)
//        {
//            string key = GetKey(taskImplementation.Name, taskImplementation.Version);
//            this.taskImplementations.Add(key, taskImplementation);
//        }
//
//        public void Start()
//        {
//            this.taskScheduler = new LimitedConcurrencyLevelTaskScheduler(this.PollThreadCount);
//            for (int i = 0; i < this.PollThreadCount; i++)
//            {
//                string threadName = string.Format("Task Worker (Tasklist = {0}, Id = {1})", this.Tasklist, i);
//                PollServiceBusTask poller = new PollServiceBusTask(threadName, this);
//                PollForTask(poller.Start);
//            }
//        }
//
//        public void Stop()
//        {
//        }
//
//        private string GetKey(string name, string version)
//        {
//            return name + "_" + version;
//        }
//
//        private void PollForTask(Action poll)
//        {
//            Task.Factory.StartNew(poll, CancellationToken.None, TaskCreationOptions.None, this.taskScheduler);
//        }
//    }
}
