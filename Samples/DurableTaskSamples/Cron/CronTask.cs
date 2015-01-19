namespace DurableTaskSamples.Cron
{
    using System;
    using System.Threading;
    using DurableTask;

    public sealed class CronTask : TaskActivity<string,string>
    {
        public CronTask()
        {
        }

        protected override string Execute(DurableTask.TaskContext context, string input)
        {
            Console.WriteLine("Executing Cron Job.  Started At: '" + DateTime.Now.ToString() + "' Number: " + input);

            Thread.Sleep(2 * 1000);

            string completed = "Cron Job '" + input + "' Completed...";
            Console.WriteLine(completed);

            return completed;
        }
    }

}
