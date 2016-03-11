namespace DurableTaskSamples.ErrorHandling
{
    using System;
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
                result = goodResult;
            }
            catch (Exception e)
            {
                hasError = true;
                Console.WriteLine($"GoodTask unexpected exception: {e}");
            }

            try
            {
                badResult = await context.ScheduleTask<string>(typeof(BadTask));
                result += badResult;
            }
            catch (TaskFailedException)
            {
                hasError = true;
                Console.WriteLine("BadTask TaskFailedException caught as expected");
            }
            catch (Exception e)
            {
                hasError = true;
                Console.WriteLine($"BadTask unexpected exception: {e}");
            }

            if (hasError && !string.IsNullOrEmpty(goodResult))
            {
                result = await context.ScheduleTask<string>(typeof(CleanupTask));
            }

            Console.WriteLine($"Orchestration Complete, result: {result}");
            return result;
        }
    }
}
