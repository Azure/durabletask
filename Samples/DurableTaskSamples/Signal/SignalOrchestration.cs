namespace DurableTaskSamples.Signal
{
    using System.Threading.Tasks;
    using DurableTask;

    public class SignalOrchestration : TaskOrchestration<string,string>
    {
        TaskCompletionSource<string> resumeHandle;

        public override async Task<string> RunTask(OrchestrationContext context, string input)
        {
            string user = await WaitForSignal();
            string greeting = await context.ScheduleTask<string>("DurableTaskSamples.Greetings.SendGreetingTask", string.Empty, user);
            return greeting;
        }

        async Task<string> WaitForSignal()
        {
            this.resumeHandle = new TaskCompletionSource<string>();
            var data = await this.resumeHandle.Task;
            this.resumeHandle = null;
            return data;
        }

        public override void OnEvent(OrchestrationContext context, string name, string input)
        {
            if (this.resumeHandle != null)
            {
                this.resumeHandle.SetResult(input);
            }
        }
    }
}