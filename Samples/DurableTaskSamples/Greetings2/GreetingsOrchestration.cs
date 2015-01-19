namespace DurableTaskSamples.Greetings2
{
    using System.Threading.Tasks;
    using DurableTask;

    public class GreetingsOrchestration2 : TaskOrchestration<string,int>
    {
        public override async Task<string> RunTask(OrchestrationContext context, int secondsToWait)
        {
            var user = context.ScheduleTask<string>("DurableTaskSamples.Greetings.GetUserTask", string.Empty);
            var timer = context.CreateTimer<string>(context.CurrentUtcDateTime.AddSeconds(secondsToWait), "TimedOut");

            var u = await Task.WhenAny(user, timer);
            string greeting = await context.ScheduleTask<string>("DurableTaskSamples.Greetings.SendGreetingTask", string.Empty, u.Result);

            return greeting;
        }
    }
}