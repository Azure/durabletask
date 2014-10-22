
namespace TaskHubStressTest
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using DurableTask;

    public class TestOrchestration : TaskOrchestration<int, TestOrchestrationData>
    {
        public override async Task<int> RunTask(OrchestrationContext context, TestOrchestrationData data)
        {
            int result = 0;
            List<Task<int>> results = new List<Task<int>>();
            int i = 0;
            int j = 0;
            for (; i < data.NumberOfParallelTasks; i++)
            {
                results.Add(context.ScheduleTask<int>(typeof(TestTask), new TestTaskData
                {
                    TaskId = "ParallelTask: " + i.ToString(),
                    MaxDelayInMinutes = data.MaxDelayInSeconds,
                }));
            }

            int[] counters = await Task.WhenAll(results.ToArray());
            result = counters.Max();

            for (; j < data.NumberOfSerialTasks; j++)
            {
                int c = await context.ScheduleTask<int>(typeof(TestTask), new TestTaskData
                {
                    TaskId = "SerialTask" + (i + j).ToString(),
                    MaxDelayInMinutes = data.MaxDelayInSeconds,
                });
                result = Math.Max(result, c);
            }

            return result;
        }
    }
}
