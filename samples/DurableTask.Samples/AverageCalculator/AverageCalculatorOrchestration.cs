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

namespace DurableTask.Samples.AverageCalculator
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using DurableTask.Core;

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

            Console.WriteLine($"Completed Average: for sum: {sum}, total: {total} = {sum/total}");

            return sum/total;
        }
    }
}
