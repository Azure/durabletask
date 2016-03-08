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

            Console.WriteLine($"SumOfSquaresOrchestration::Start::{input}");

            int sum = 0;
            var chunks = new List<Task<int>>();
            var resultChunks = new List<int>();
            JArray data = JArray.Parse(input);

            foreach (var item in data)
            {
                Console.WriteLine($"Array Item::{item.Type}::{item.ToString(Newtonsoft.Json.Formatting.None)}");
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

            Console.WriteLine($"Wait for {chunks.Count} tasks");

            if (chunks.Count > 0)
            {
                var allChunks = await Task.WhenAll(chunks.ToArray());
                foreach (int result in allChunks)
                {
                    Console.WriteLine($"Add(a): {result}");
                    sum += result;
                }
            }

            foreach (int result in resultChunks)
            {
                Console.WriteLine($"Add(s): {result}");
                sum += result;
            }
            
            Console.WriteLine($"Sum of Squares: {sum}");
            Console.WriteLine($"SumOfSquaresOrchestration::End");

            return sum;
        }

    }
}
