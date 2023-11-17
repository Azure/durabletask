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
    using System.Threading.Tasks;
    using DurableTask.Core;
    using TestApplication.Common.OrchestrationTasks;

    public class SimpleRecurringSubOrchestration : TaskOrchestration<string, int>
    {
        public override async Task<string> RunTask(OrchestrationContext context, int recurrenceCount)
        {
            IUserTasks userTasks = context.CreateClient<IUserTasks>();
            string subOrchOutput = string.Empty;
            if (recurrenceCount > 0)
            {
                var subSubOrch1 = context.CreateSubOrchestrationInstance<string>(typeof(SimpleRecurringSubOrchestration), --recurrenceCount);
                var myTask = userTasks.GreetUserAsync("World");
                await Task.WhenAll(subSubOrch1, myTask);
                subOrchOutput = subSubOrch1.Result;
            }

            return $"Level = {recurrenceCount};{subOrchOutput}";
        }

        public static string GetExpectedResult(int recurrenceCount)
        {
            string subOrchOutput = string.Empty;
            if (recurrenceCount > 0)
            {
                subOrchOutput = GetExpectedResult(--recurrenceCount);
            }

            return $"Level = {recurrenceCount};{subOrchOutput}";
        }
    }
}
