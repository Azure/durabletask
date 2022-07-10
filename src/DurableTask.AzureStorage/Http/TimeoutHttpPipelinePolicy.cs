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
#nullable enable
namespace DurableTask.AzureStorage.Http
{
    using System;
    using System.Threading.Tasks;
    using Azure.Core;
    using Azure.Core.Pipeline;
    using DurableTask.AzureStorage.Logging;

    sealed class TimeoutHttpPipelinePolicy : HttpPipelinePolicy
    {
        public event EventHandler<HttpMessage>? Timeout;

        static (DateTime Last, int Count) timeouts;

        readonly AzureStorageOrchestrationServiceSettings settings;

        public TimeoutHttpPipelinePolicy(AzureStorageOrchestrationServiceSettings settings)
        {
            this.settings = settings ?? throw new ArgumentNullException(nameof(settings));
        }

        public override void Process(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            try
            {
                ProcessNext(message, pipeline);
            }
            catch (OperationCanceledException)
            {
                OnTimeout(message);
                throw;
            }
        }

        public override async ValueTask ProcessAsync(HttpMessage message, ReadOnlyMemory<HttpPipelinePolicy> pipeline)
        {
            try
            {
                await ProcessNextAsync(message, pipeline);
            }
            catch (OperationCanceledException)
            {
                OnTimeout(message);
                throw;
            }
        }

        private static void OnTimeout(HttpMessage message)
        {

            // If more less than DefaultTimeoutCooldown passed, increase timeouts count
            // otherwise, reset the count to 1, since this is the first timeout we receive
            // after a long (enough) while
            if (LastTimeoutHit + settings.StorageRequestsTimeoutCooldown > DateTime.UtcNow)
            {
                NumTimeoutsHit++;
            }
            else
            {
                NumTimeoutsHit = 1;
            }

            LastTimeoutHit = DateTime.UtcNow;

            string taskHubName = settings?.TaskHubName;
            if (NumTimeoutsHit < settings.MaxNumberOfTimeoutsBeforeRecycle)
            {
                string message = $"The operation '{operationName}' with id '{context.ClientRequestID}' did not complete in '{settings.StorageRequestsTimeout}'. Hit {NumTimeoutsHit} out of {settings.MaxNumberOfTimeoutsBeforeRecycle} allowed timeouts. Retrying the operation.";
                settings.Logger.GeneralWarning(account ?? "", taskHubName ?? "", message);

                cts.Cancel();
                continue;
            }
            else
            {
                string message = $"The operation '{operationName}' with id '{context.ClientRequestID}' did not complete in '{settings.StorageRequestsTimeout}'. Hit maximum number ({settings.MaxNumberOfTimeoutsBeforeRecycle}) of timeouts. Terminating the process to mitigate potential deadlock.";
                settings.Logger.GeneralError(account ?? "", taskHubName ?? "", message);

                // Delay to ensure the ETW event gets written
                await Task.Delay(TimeSpan.FromSeconds(3));

                bool executeFailFast = true;
                Task<bool> gracefulShutdownTask = Task.Run(async () =>
                {
                    try
                    {
                        return await settings.OnImminentFailFast(message);
                    }
                    catch (Exception)
                    {
                        return true;
                    }
                });

                await Task.WhenAny(gracefulShutdownTask, Task.Delay(TimeSpan.FromSeconds(35)));

                if (gracefulShutdownTask.IsCompleted)
                {
                    executeFailFast = gracefulShutdownTask.Result;
                }

                if (executeFailFast)
                {
                    TimeoutHandler.ProcessKillAction(message);
                }
                else
                {
                    // Technically we don't need else as the action above would have killed the process.
                    // However tests don't kill the process so putting in else.
                    throw new TimeoutException(message);
                }
            }
        }
    }
}
