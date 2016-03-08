namespace DurableTaskSamples.SumOfSquares
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using DurableTask;
    using Newtonsoft.Json.Linq;

    public class SumOfSquaresOrchestrationAsTask : TaskOrchestration<int, string>
    {
        public override async Task<int> RunTask(OrchestrationContext context, string input)
        {
            if (string.IsNullOrEmpty(input))
            {
                throw new ArgumentException(nameof(input));
            }

            Console.WriteLine($"SumOfSquaresOrchestrationAsTask::Start::{input}");

            int chunk = Convert.ToInt32(input.Replace("[", "").Replace("]", ""));

            DateTime currentTime = context.CurrentUtcDateTime;
            DateTime fireAt = currentTime.AddSeconds(1);
            await context.CreateTimer<string>(fireAt, "done");

            Console.WriteLine($"Square(Orch)::{chunk}::{chunk * chunk}");
            Console.WriteLine($"SumOfSquaresOrchestrationAsTask::End");

            return chunk * chunk;
        }
    }
}
