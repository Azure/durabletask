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
    using TestApplication.Common.OrchestrationTasks;

    public class SimpleOrchestrationWithSubOrchestration : TaskOrchestration<string, string>
    {
        public override async Task<string> RunTask(OrchestrationContext context, string input)
        {
            IUserTasks userTasks = context.CreateClient<IUserTasks>();
            var subOrch1 = context.CreateSubOrchestrationInstance<string>(typeof(SimpleOrchestrationWithTasks), input);
            var subOrch2 = context.CreateSubOrchestrationInstance<string>(typeof(SimpleOrchestrationWithTasks), input);
            var myTask = userTasks.GreetUserAsync("World");
            await Task.WhenAll(subOrch1, subOrch2, myTask);
            return $"TaskResult = {myTask.Result} , SubOrchestration1Result = {subOrch1.Result}, SubOrchestration2Result = {subOrch2.Result}";
        }
    }
}
