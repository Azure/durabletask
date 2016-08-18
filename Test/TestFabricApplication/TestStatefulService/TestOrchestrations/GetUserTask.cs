using DurableTask;

namespace TestStatefulService.TestOrchestrations
{
    class GetUserTask : TaskActivity<string, string>
    {
        protected override string Execute(TaskContext context, string input)
        {
            return "Gabbar";
        }
    }
}
