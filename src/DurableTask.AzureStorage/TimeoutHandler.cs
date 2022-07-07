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

namespace DurableTask.AzureStorage
{
    using DurableTask.AzureStorage.Monitoring;
    using Microsoft.WindowsAzure.Storage;
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    // Class that acts as a timeout handler to wrap Azure Storage calls, mitigating a deadlock that occurs with Azure Storage SDK 9.3.3.
    // The TimeoutHandler class is based off of the similar Azure Functions fix seen here: https://github.com/Azure/azure-webjobs-sdk/pull/2291
    internal static class TimeoutHandler
    {
        private static int NumTimeoutsHit = 0;

        private static DateTime LastTimeoutHit = DateTime.MinValue;

        /// <summary>
        /// Process kill action. This is exposed here to allow override from tests.
        /// </summary>
        private static Action<string> ProcessKillAction = (errorMessage) => Environment.FailFast(errorMessage);

        public static async Task<T> ExecuteWithTimeout<T>(
            string operationName,
            string account,
            AzureStorageOrchestrationServiceSettings settings,
            Func<OperationContext, CancellationToken, Task<T>> operation,
            AzureStorageOrchestrationServiceStats stats = null,
            string clientRequestId = null)
        {
            OperationContext context = new OperationContext() { ClientRequestID = clientRequestId ?? Guid.NewGuid().ToString() };
            if (Debugger.IsAttached)
            {
                // ignore long delays while debugging
                return await operation(context, CancellationToken.None);
            }

            while (true)
            {
                using (var cts = new CancellationTokenSource())
                {
                    Task timeoutTask = Task.Delay(settings.StorageRequestsTimeout, cts.Token);
                    Task<T> operationTask = operation(context, cts.Token);

                    if (stats != null)
                    {
                        stats.StorageRequests.Increment();
                    }
                    Task completedTask = await Task.WhenAny(timeoutTask, operationTask);

                    if (Equals(timeoutTask, completedTask))
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

                    cts.Cancel();

                    return await operationTask;
                }
            }
        }
    }
}
