namespace DurableTaskSamples.SumOfSquares
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using DurableTask;
    using Newtonsoft.Json.Linq;

    public class SumOfSquaresOrchestration : TaskOrchestration<int, string>
    {
        public override async Task<int> RunTask(OrchestrationContext context, string input)
        {
            if (string.IsNullOrEmpty(input))
            {
                throw new ArgumentException(nameof(input));
            }

            int sum = 0;
            var chunks = new List<Task<int>>();
            var resultChunks = new List<int>();
            JArray data = JArray.Parse(input);

            foreach (var item in data)
            {
                // use resultchunks for sync processing, chunks for async
                switch (item.Type)
                {
                    case JTokenType.Array:
                        var subOrchestration = context.CreateSubOrchestrationInstance<int>(typeof(SumOfSquaresOrchestration), item.ToString(Newtonsoft.Json.Formatting.None));
                        chunks.Add(subOrchestration);
                        //resultChunks.Add(await context.CreateSubOrchestrationInstance<int>(typeof(SumOfSquaresOrchestration), item.ToString(Newtonsoft.Json.Formatting.None)));
                        break;
                    case JTokenType.Integer:
                        var activity = context.ScheduleTask<int>(typeof(SumOfSquaresTask), (int)item);
                        chunks.Add(activity);
                        //resultChunks.Add(await context.ScheduleTask<int>(typeof(SumOfSquaresTask), (int)item));
                        break;
                    case JTokenType.Comment:
                        break;
                    default:
                        throw new InvalidOperationException($"Invalid input: {item.Type}");
                }
            }

            if (chunks.Count > 0)
            {
                var allChunks = await Task.WhenAll(chunks.ToArray());
                foreach (int result in allChunks)
                {
                    sum += result;
                }
            }

            foreach (int result in resultChunks)
            {
                sum += result;
            }
            
            Console.WriteLine($"Sum of Squares: {sum}");

            return sum;
        }

    }
}
