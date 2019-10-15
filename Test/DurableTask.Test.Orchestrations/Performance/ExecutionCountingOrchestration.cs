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
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using DurableTask.Core;

    public sealed class ExecutionCountingOrchestration : TaskOrchestration<int, int>
    {
        public override async Task<int> RunTask(OrchestrationContext context, int numberOfActivities)
        {
            List<Task<int>> results = new List<Task<int>>();
            for (int i = 0; i < numberOfActivities; i++)
            {
                results.Add(context.ScheduleTask<int>(typeof(ExecutionCountingActivity), i));
            }

            await Task.WhenAll(results);
            return ExecutionCountingActivity.Counter;
        }
    }
}
