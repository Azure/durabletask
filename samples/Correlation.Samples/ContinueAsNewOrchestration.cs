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

    [KnownType(typeof(HelloActivity))]
    internal class ContinueAsNewOrchestration : TaskOrchestration<string, string>
    {
        static int counter = 0;

        public override async Task<string> RunTask(OrchestrationContext context, string input)
        {
            string result = await context.ScheduleTask<string>(typeof(HelloActivity), input);
            result = input + ":" + result;
            if (counter < 3)
            {
                counter++;
                context.ContinueAsNew(result);
            }

            return result;
        }
    }

    internal class HelloActivity : TaskActivity<string, string>
    {
        protected override string Execute(TaskContext context, string input)
        {
            if (string.IsNullOrEmpty(input))
            {
                throw new ArgumentNullException(nameof(input));
            }

            Console.WriteLine($"Activity: Hello {input}");
            return $"Hello, {input}!";
        }
    }
}
