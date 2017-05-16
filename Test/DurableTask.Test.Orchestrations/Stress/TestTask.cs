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

namespace DurableTask.Test.Orchestrations.Stress
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;

    public sealed class TestTask : AsyncTaskActivity<TestTaskData, int>
    {
        public int counter = 0;

        public TestTask()
        {
        }

        protected override async Task<int> ExecuteAsync(TaskContext context, TestTaskData input)
        {
            int c = Interlocked.Increment(ref this.counter);
            OrchestrationInstance instance = context.OrchestrationInstance;
            Random random = new Random();
            int minutesToSleep = random.Next(0, input.MaxDelayInMinutes);

            Console.WriteLine(string.Format("[InstanceId: {0}, ExecutionId: {1}, TaskId: {2}, Counter: {3}] ---> Sleeping for '{4}'", 
                instance.InstanceId, instance.ExecutionId, input.TaskId, c, minutesToSleep));

            await Task.Delay(TimeSpan.FromMinutes(minutesToSleep));

            return c;
        }

    }
}
