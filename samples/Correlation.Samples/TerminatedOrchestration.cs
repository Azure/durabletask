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
    using System.Threading.Tasks;
    using DurableTask.Core;

    [KnownType(typeof(WaitActivity))]
    internal class TerminatedOrchestration : TaskOrchestration<string, string>
    {
        public override async Task<string> RunTask(OrchestrationContext context, string input)
        {
            return await context.ScheduleTask<string>(typeof(WaitActivity), "");
        }
    }

    internal class WaitActivity : TaskActivity<string, string>
    {
        protected override string Execute(TaskContext context, string input)
        {
            return input;
        }

        protected override async Task<string> ExecuteAsync(TaskContext context, string input) {
            // Wait for 5 min for terminate. 
            await Task.Delay(TimeSpan.FromMinutes(2));

            Console.WriteLine($"Activity: Hello {input}");
            return $"Hello, {input}!";
        }
    }
}
