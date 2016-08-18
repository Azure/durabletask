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
