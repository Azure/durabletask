namespace DurableTaskSamples.ErrorHandling
{
    using System.Threading.Tasks;
    using DurableTask;
    using DurableTask.Exceptions;

    public class ErrorHandlingOrchestration : TaskOrchestration<string, string>
    {
        public override async Task<string> RunTask(OrchestrationContext context, string input)
        {
            string goodResult = null;
            string badResult = null;
            string result = null;
            bool hasError = false;
            try
            {
                goodResult = await context.ScheduleTask<string>(typeof(GoodTask));
                badResult = await context.ScheduleTask<string>(typeof(BadTask));
                result = goodResult + badResult;
            }
            catch (TaskFailedException)
            {
                hasError = true;
            }

            if (hasError && !string.IsNullOrEmpty(goodResult))
            {
                result = await context.ScheduleTask<string>(typeof(CleanupTask));
            }

            return result;
        }
    }
}
