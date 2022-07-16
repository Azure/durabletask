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

    [KnownType(typeof(MultiLayeredOrchestrationChildWithRetry))]
    [KnownType(typeof(NeedToExecuteTwice01))]
    [KnownType(typeof(NeedToExecuteTwice02))]
    class MultiLayeredOrchestrationWithRetryOrchestrator : TaskOrchestration<string, string>
    {
        public override Task<string> RunTask(OrchestrationContext context, string input)
        {
            var retryOption = new RetryOptions(TimeSpan.FromMilliseconds(10), 3);
            return context.CreateSubOrchestrationInstanceWithRetry<string>(typeof(MultiLayeredOrchestrationChildWithRetry), retryOption, input);
        }
    }

    [KnownType(typeof(NeedToExecuteTwice01))]
    [KnownType(typeof(NeedToExecuteTwice02))]
    class MultiLayeredOrchestrationChildWithRetry : TaskOrchestration<string, string>
    {
        public override async Task<string> RunTask(OrchestrationContext context, string input)
        {
            var result01 = await context.ScheduleTask<string>(typeof(NeedToExecuteTwice01), input);
            var result02 = await context.ScheduleTask<string>(typeof(NeedToExecuteTwice02), input);
            return $"{result01}:{result02}";
        }
    }

    class NeedToExecuteTwice01 : TaskActivity<string, string>
    {
        static int Counter = 0;

        protected override string Execute(TaskContext context, string input)
        {
            if (Counter == 0)
            {
                Counter++;
                throw new Exception("Something happens");
            }

            return $"Hello {input} with retry";
        }
    }

    class NeedToExecuteTwice02 : TaskActivity<string, string>
    {
        static int Counter = 0;

        protected override string Execute(TaskContext context, string input)
        {
            if (Counter == 0)
            {
                Counter++;
                throw new Exception("Something happens");
            }

            return $"Hello {input} with retry";
        }
    }
}
