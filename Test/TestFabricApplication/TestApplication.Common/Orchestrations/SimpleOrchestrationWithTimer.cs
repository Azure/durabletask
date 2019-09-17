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
    using System;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using TestApplication.Common.OrchestrationTasks;

    public class SimpleOrchestrationWithTimer : TaskOrchestration<string, int>
    {
        public override async Task<string> RunTask(OrchestrationContext context, int input)
        {
            IUserTasks userTasks = context.CreateClient<IUserTasks>();
            await context.CreateTimer<object>(context.CurrentUtcDateTime.Add(TimeSpan.FromSeconds(input)), null);
            return await userTasks.GreetUserAsync("Gabbar");
        }
    }
}
