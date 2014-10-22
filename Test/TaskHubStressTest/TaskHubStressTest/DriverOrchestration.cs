namespace TaskHubStressTest
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using DurableTask;

    public class DriverOrchestration : TaskOrchestration<int, DriverOrchestrationData>
    {
        public override async Task<int> RunTask(OrchestrationContext context, DriverOrchestrationData data)
        {
            int result = 0;
            List<Task<int>> results = new List<Task<int>>();
            int i = 0;
            for (; i < data.NumberOfParallelTasks; i++)
            {
                results.Add(context.CreateSubOrchestrationInstance<int>(typeof(TestOrchestration), data.SubOrchestrationData));
            }

            int[] counters = await Task.WhenAll(results.ToArray());
            result = counters.Max();

            if (data.NumberOfIteration > 0)
            {
                data.NumberOfIteration--;
                context.ContinueAsNew(data);
            }

            return result;
        }
    }
}
