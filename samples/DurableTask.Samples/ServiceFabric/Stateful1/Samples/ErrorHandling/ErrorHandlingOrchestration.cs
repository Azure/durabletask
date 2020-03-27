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

using System;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.Exceptions;

namespace Stateful1.Samples.ErrorHandling
{
    public class ErrorHandlingOrchestration : TaskOrchestration<string, string>
    {
        public override async Task<string> RunTask(OrchestrationContext context, string input)
        {
            string goodResult = null;
            string badResult = null;
            string result = null;
            bool hasError = false;

            try
            {
                goodResult = await context.ScheduleTask<string>(typeof(GoodTask));
                result = goodResult;
            }
            catch (Exception e)
            {
                hasError = true;
                ServiceEventSource.Current.Message($"GoodTask unexpected exception: {e}");
            }

            try
            {
                badResult = await context.ScheduleTask<string>(typeof(BadTask));
                result += badResult;
            }
            catch (TaskFailedException)
            {
                hasError = true;
                ServiceEventSource.Current.Message("BadTask TaskFailedException caught as expected");
            }
            catch (Exception e)
            {
                hasError = true;
                ServiceEventSource.Current.Message($"BadTask unexpected exception: {e}");
            }

            if (hasError && !string.IsNullOrWhiteSpace(goodResult))
            {
                result = await context.ScheduleTask<string>(typeof(CleanupTask));
            }

            ServiceEventSource.Current.Message($"Orchestration Complete, result: {result}");
            return result;
        }
    }
}
