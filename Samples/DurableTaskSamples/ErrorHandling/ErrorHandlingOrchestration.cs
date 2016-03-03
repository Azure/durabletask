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
                badResult = await context.ScheduleTask<string>(typeof(BadTask));
                result = goodResult + badResult;
            }
            catch (TaskFailedException)
            {
                hasError = true;
                Console.WriteLine("TaskFailedException caught as expected");
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
