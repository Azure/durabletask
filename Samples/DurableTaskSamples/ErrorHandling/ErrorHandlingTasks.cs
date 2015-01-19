namespace DurableTaskSamples.ErrorHandling
{
    using System;
    using DurableTask;

    public sealed class GoodTask : TaskActivity<string, string>
    {
        public GoodTask()
        {
        }

        protected override string Execute(DurableTask.TaskContext context, string input)
        {
            Console.WriteLine("GoodTask Executed...");

            return "GoodResult";
        }
    }

    public sealed class BadTask : TaskActivity<string, string>
    {
        public BadTask()
        {
        }

        protected override string Execute(DurableTask.TaskContext context, string input)
        {
            Console.WriteLine("BadTask Executed...");

            throw new InvalidOperationException("BadTask failed.");
        }
    }

    public sealed class CleanupTask : TaskActivity<string, string>
    {
        public CleanupTask()
        {
        }

        protected override string Execute(DurableTask.TaskContext context, string input)
        {
            Console.WriteLine("CleanupTask Executed...");

            return "CleanupResult";
        }
    }

}