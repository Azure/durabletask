namespace DurableTaskSamples.Greetings
{
    using System.Threading.Tasks;
    using DurableTask;

    public class GreetingsOrchestration : TaskOrchestration<string,string>
    {
        public override async Task<string> RunTask(OrchestrationContext context, string input)
        {
            string user = await context.ScheduleTask<string>(typeof(GetUserTask));
            string greeting = await context.ScheduleTask<string>(typeof(SendGreetingTask), user);
            return greeting;
        }
    }
}