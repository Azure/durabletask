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

namespace Correlation.Samples
{
    using System;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Core;

    [KnownType(typeof(ParallelHello))]
    internal class FanOutFanInOrchestrator : TaskOrchestration<string, string>
    {
        public override async Task<string> RunTask(OrchestrationContext context, string input)
        {
            var tasks = new Task<string>[3];
            for (int i = 0; i < 3; i++)
            {
                tasks[i] = context.ScheduleTask<string>(typeof(ParallelHello), $"world({i})");
            }

            await Task.WhenAll(tasks);
            var buffer = new StringBuilder();
            foreach(var task in tasks)
            {
                buffer.Append(task.Result);
                buffer.Append(":");
            }

            return buffer.ToString();
        }
    }

    internal class ParallelHello : TaskActivity<string, string>
    {
        protected override string Execute(TaskContext context, string input)
        {
            if (string.IsNullOrEmpty(input))
            {
                throw new ArgumentNullException(nameof(input));
            }

            Console.WriteLine($"Activity: Parallel Hello {input}");
            return $"Parallel Hello, {input}!";
        }
    }
}
