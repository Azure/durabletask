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

namespace DurableTask.Samples.Signal
{
    using System.Threading.Tasks;
    using DurableTask.Core;

    public class SignalOrchestration : TaskOrchestration<string,string>
    {
        TaskCompletionSource<string> resumeHandle;

        public override async Task<string> RunTask(OrchestrationContext context, string input)
        {
            string user = await WaitForSignal();
            string greeting = await context.ScheduleTask<string>("DurableTask.Samples.Greetings.SendGreetingTask", string.Empty, user);
            return greeting;
        }

        async Task<string> WaitForSignal()
        {
            this.resumeHandle = new TaskCompletionSource<string>();
            var data = await this.resumeHandle.Task;
            this.resumeHandle = null;
            return data;
        }

        public override void OnEvent(OrchestrationContext context, string name, string input)
        {
            if (this.resumeHandle != null)
            {
                this.resumeHandle.SetResult(input);
            }
        }
    }
}