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
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;

    public sealed class ExecutionCountingActivity : AsyncTaskActivity<int, int>
    {
        // This has to be reset before starting the orchestration and the orchestration
        // must be run in sequence for this to determine the correct number of times all
        // the activities together executed.
        public static int Counter = 0;

        protected override async Task<int> ExecuteAsync(TaskContext context, int taskId)
        {
            await Task.Delay(new Random().Next(50, 100));
            Interlocked.Increment(ref Counter);
            return Counter;
        }
    }
}
