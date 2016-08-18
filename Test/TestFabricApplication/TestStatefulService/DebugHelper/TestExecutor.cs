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
using System.Threading;
using System.Threading.Tasks;
using DurableTask;
using TestStatefulService.TestOrchestrations;

namespace TestStatefulService.DebugHelper
{
    class TestExecutor
    {
        TaskHubClient client;
        CancellationTokenSource cancellationTokenSource;

        public TestExecutor(TaskHubClient client)
        {
            this.client = client;
            this.cancellationTokenSource = new CancellationTokenSource();
        }

        public Task StartAsync()
        {
            var startTask = Task.Run(async () =>
            {
                var instance = await client.CreateOrchestrationInstanceAsync(typeof(SimpleOrchestrationWithTasks), input: null);
                var state = await client.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(10));
                var result = state.Output;
            }, this.cancellationTokenSource.Token);

            return Task.FromResult<object>(null);
        }

        public Task StopAsync()
        {
            this.cancellationTokenSource.Cancel();
            return Task.FromResult<object>(null);
        }
    }
}
