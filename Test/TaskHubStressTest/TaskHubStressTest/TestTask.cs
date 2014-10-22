namespace TaskHubStressTest
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask;

    public sealed class TestTask : AsyncTaskActivity<TestTaskData, int>
    {
        public int counter = 0;

        public TestTask()
        {
        }

        protected override async Task<int> ExecuteAsync(TaskContext context, TestTaskData input)
        {
            int c = Interlocked.Increment(ref this.counter);
            OrchestrationInstance instance = context.OrchestrationInstance;
            Random random = new Random();
            int minutesToSleep = random.Next(0, input.MaxDelayInMinutes);

            Console.WriteLine(string.Format("[InstanceId: {0}, ExecutionId: {1}, TaskId: {2}, Counter: {3}] ---> Sleeping for '{4}'", 
                instance.InstanceId, instance.ExecutionId, input.TaskId, c, minutesToSleep));

            await Task.Delay(TimeSpan.FromMinutes(minutesToSleep));

            return c;
        }

    }
}
