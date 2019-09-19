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

namespace TestApplication.Common.Orchestrations
{
    using System;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Exceptions;
    using TestApplication.Common.OrchestrationTasks;

    public class OrchestrationRunningIntoRetry : TaskOrchestration<int, int>
    {
        CounterException LatestException;

        public override async Task<int> RunTask(OrchestrationContext context, int numberOfRetriesToEnforce)
        {
            var retryOptions = new RetryOptions(TimeSpan.FromSeconds(1), 5)
            {
                BackoffCoefficient = 2,
                MaxRetryInterval = TimeSpan.FromMinutes(1),
                Handle = RetryExceptionHandler
            };
            ITestTasks testTasks = context.CreateRetryableClient<ITestTasks>(retryOptions);
            var result = await testTasks.ThrowExceptionAsync(this.LatestException?.Counter -1 ?? numberOfRetriesToEnforce);

            if (result)
            {
                return numberOfRetriesToEnforce;
            }

            throw new Exception($"Unexpected exception thrown from {nameof(OrchestrationRunningIntoRetry)}.");
        }

        bool RetryExceptionHandler(Exception e)
        {
            TaskFailedException tfe = e as TaskFailedException;

            if (tfe != null && tfe.InnerException != null)
            {
                e = tfe.InnerException;
            }

            CounterException ce = e as CounterException;
            if (ce != null)
            {
                LatestException = ce;
                return true;
            }

            return false;
        }
    }
}
