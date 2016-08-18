using DurableTask;

namespace TestStatefulService.TestOrchestrations
{
    class GreetUserTask : TaskActivity<string, string>
    {
        protected override string Execute(TaskContext context, string user)
        {
            return "Hello " + user;
        }
    }
}
