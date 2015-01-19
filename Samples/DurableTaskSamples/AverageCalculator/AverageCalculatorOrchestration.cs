namespace DurableTaskSamples.AverageCalculator
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using DurableTask;

    public class AverageCalculatorOrchestration : TaskOrchestration<double, int[]>
    {
        public override async Task<double> RunTask(OrchestrationContext context, int[] input)
        {
            if (input == null || input.Length != 3)
            {
                throw new ArgumentException("input");
            }

            int start = input[0];
            int end = input[1];
            int step = input[2];
            int total = end - start + 1;

            var chunks = new List<Task<int>>();
            int current;
            while (start < end)
            {
                current = start + step - 1;
                if (current > end)
                {
                    current = end;
                }

                var chunk = context.ScheduleTask<int>(typeof(ComputeSumTask), new int[] {start, current});
                chunks.Add(chunk);

                start = current + 1;
            }

            int sum = 0;
            int[] allChunks = await Task.WhenAll(chunks.ToArray());
            foreach (int result in allChunks)
            {
                sum += result;
            }

            return sum/total;
        }
    }
}
