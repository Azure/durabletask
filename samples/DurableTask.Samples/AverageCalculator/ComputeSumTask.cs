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
    using DurableTask.Core;

    public sealed class ComputeSumTask : TaskActivity<int[], int>
    {
        public ComputeSumTask()
        {
        }

        protected override int Execute(DurableTask.Core.TaskContext context, int[] chunk)
        {
            if (chunk == null || chunk.Length != 2)
            {
                throw new ArgumentException("chunk");
            }

            Console.WriteLine("Compute Sum for " + chunk[0] + "," + chunk[1]);
            int sum = 0;
            int start = chunk[0];
            int end = chunk[1];
            for (int i = start; i <= end; i++)
            {
                sum += i;
            }

            Console.WriteLine("Total Sum for Chunk '" + chunk[0] + "," + chunk[1] + "' is " + sum.ToString());

            return sum;
        }
    }

}
