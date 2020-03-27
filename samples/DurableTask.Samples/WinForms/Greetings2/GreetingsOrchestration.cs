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

namespace DurableTask.Samples.Greetings2
{
    using System.Threading.Tasks;
    using DurableTask.Core;

    public class GreetingsOrchestration2 : TaskOrchestration<string,int>
    {
        public override async Task<string> RunTask(OrchestrationContext context, int secondsToWait)
        {
            Task<string> user = context.ScheduleTask<string>("DurableTaskSamples.Greetings.GetUserTask", string.Empty);
            Task<string> timer = context.CreateTimer(context.CurrentUtcDateTime.AddSeconds(secondsToWait), "TimedOut");

            Task<string> u = await Task.WhenAny(user, timer);
            string greeting = await context.ScheduleTask<string>("DurableTaskSamples.Greetings.SendGreetingTask", string.Empty, u.Result);

            return greeting;
        }
    }
}