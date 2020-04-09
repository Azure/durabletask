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

namespace DurableTask.Test.Orchestrations.Performance
{
    using System;
    using System.Threading.Tasks;
    using DurableTask.Core;

    public sealed class RandomTimeWaitingTask : AsyncTaskActivity<RandomTimeWaitingTaskInput, int>
    {
        protected override async Task<int> ExecuteAsync(TaskContext context, RandomTimeWaitingTaskInput input)
        {
            int delayTime;

            if (input.MaxDelay == input.MinDelay)
            {
                delayTime = input.MaxDelay;
            }
            else
            {
                Random random = new Random();
                delayTime = random.Next(input.MinDelay, input.MaxDelay);
            }

            // Uncomment this block to force some failures.
            //if (new Random().NextDouble() < 0.1)
            //{
            //    throw new Exception();
            //}

            await Task.Delay(TimeSpan.FromMilliseconds(input.DelayUnit.TotalMilliseconds * delayTime));

            return 1;
        }
    }
}
