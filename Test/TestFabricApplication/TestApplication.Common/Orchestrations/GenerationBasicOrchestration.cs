﻿//  ----------------------------------------------------------------------------------
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

    public class GenerationBasicOrchestration : TaskOrchestration<int, int>
    {
        // HACK: This is just a hack to communicate result of orchestration back to test
        public static int Result;

        public override async Task<int> RunTask(OrchestrationContext context, int numberOfGenerations)
        {
            int count = await context.ScheduleTask<int>(typeof(GenerationBasicTask));
            numberOfGenerations--;
            if (numberOfGenerations > 0)
            {
                context.ContinueAsNew(numberOfGenerations);
            }

            // This is a HACK to get unit test up and running.  Should never be done in actual code.
            Result = count;
            return count;
        }
    }

    public sealed class GenerationBasicTask : TaskActivity<string, int>
    {
        // HACK: This is just a hack to communicate result of orchestration back to test
        public static int GenerationCount = 0;

        protected override int Execute(TaskContext context, string input)
        {
            GenerationCount++;
            return GenerationCount;
        }
    }
}
