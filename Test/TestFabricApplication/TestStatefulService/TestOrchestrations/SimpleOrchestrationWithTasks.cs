using System.Threading.Tasks;
using DurableTask;

namespace TestStatefulService.TestOrchestrations
{
    class SimpleOrchestrationWithTasks : TaskOrchestration<string, string>
    {
        public override async Task<string> RunTask(OrchestrationContext context, string input)
        {
            string user = await context.ScheduleTask<string>(typeof(GetUserTask));

            string greeting = await context.ScheduleTask<string>(typeof(GreetUserTask), user);

            return greeting;
        }
    }
}
