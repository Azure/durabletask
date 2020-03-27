//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

namespace DurableTask.Samples.SumOfSquares
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using Newtonsoft.Json.Linq;

    public class SumOfSquaresOrchestration : TaskOrchestration<int, string>
    {
        public override async Task<int> RunTask(OrchestrationContext context, string input)
        {
            if (string.IsNullOrWhiteSpace(input))
            {
                throw new ArgumentException(nameof(input));
            }

            var sum = 0;
            var chunks = new List<Task<int>>();
            JArray data = JArray.Parse(input);
            ////var resultChunks = new List<int>();

            foreach (JToken item in data)
            {
                // use resultChunks for sync processing, chunks for async
                switch (item.Type)
                {
                    case JTokenType.Array:
                        Task<int> subOrchestration = context.CreateSubOrchestrationInstance<int>(typeof(SumOfSquaresOrchestration), item.ToString(Newtonsoft.Json.Formatting.None));
                        chunks.Add(subOrchestration);
                        ////resultChunks.Add(await context.CreateSubOrchestrationInstance<int>(typeof(SumOfSquaresOrchestration), item.ToString(Newtonsoft.Json.Formatting.None)));
                        break;
                    case JTokenType.Integer:
                        Task<int> activity = context.ScheduleTask<int>(typeof(SumOfSquaresTask), (int)item);
                        chunks.Add(activity);
                        ////resultChunks.Add(await context.ScheduleTask<int>(typeof(SumOfSquaresTask), (int)item));
                        break;
                    case JTokenType.Comment:
                        break;
                    default:
                        throw new InvalidOperationException($"Invalid input: {item.Type}");
                }
            }

            if (chunks.Count > 0)
            {
                int[] allChunks = await Task.WhenAll(chunks.ToArray());
                foreach (int result in allChunks)
                {
                    sum += result;
                }
            }

            ////foreach (int result in resultChunks)
            ////{
            ////    sum += result;
            ////}
            
            Console.WriteLine($"Sum of Squares: {sum}");

            return sum;
        }
    }
}
