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
    using Microsoft.WindowsAzure.Storage;
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    // Class that acts as a timeout handler to wrap Azure Storage calls, mitigating a deadlock that occurs with Azure Storage SDK 9.3.3.
    // The TimeoutHandler class is based off of the similar Azure Functions fix seen here: https://github.com/Azure/azure-webjobs-sdk/pull/2291
    internal static class TimeoutHandler
    {
        // The number of times we allow the timeout to be hit before recycling the app. We set this
        // to a fixed value to prevent building up an infinite number of deadlocked tasks and leak resources.
        private const int MaxNumberOfTimeoutsBeforeRecycle = 5;

        private static readonly TimeSpan DefaultTimeout = TimeSpan.FromMinutes(2);

        private static int NumTimeoutsHit = 0;

        /// <summary>
        /// Process kill action. This is exposed here to allow override from tests.
        /// </summary>
        private static Action<string> ProcessKillAction = (errorMessage) => Environment.FailFast(errorMessage);

        public static async Task<T> ExecuteWithTimeout<T>(
            string operationName,
            string account,
            AzureStorageOrchestrationServiceSettings settings,
            Func<OperationContext, CancellationToken, Task<T>> operation)
        {
            OperationContext context = new OperationContext() { ClientRequestID = Guid.NewGuid().ToString() };
            if (Debugger.IsAttached)
            {
                // ignore long delays while debugging
                return await operation(context, CancellationToken.None);
            }

            while (true)
            {
                using (var cts = new CancellationTokenSource())
                {
                    Task timeoutTask = Task.Delay(DefaultTimeout, cts.Token);
                    Task<T> operationTask = operation(context, cts.Token);

                    Task completedTask = await Task.WhenAny(timeoutTask, operationTask);

                    if (Equals(timeoutTask, completedTask))
                    {
                        NumTimeoutsHit++;
                        string taskHubName = settings?.TaskHubName;
                        if (NumTimeoutsHit < MaxNumberOfTimeoutsBeforeRecycle)
                        {
                            string message = $"The operation '{operationName}' with id '{context.ClientRequestID}' did not complete in '{DefaultTimeout}'. Hit {NumTimeoutsHit} out of {MaxNumberOfTimeoutsBeforeRecycle} allowed timeouts. Retrying the operation.";
                            settings.Logger.GeneralWarning(account ?? "", taskHubName ?? "", message);

                            cts.Cancel();
                            continue;
                        }
                        else
                        {
                            string message = $"The operation '{operationName}' with id '{context.ClientRequestID}' did not complete in '{DefaultTimeout}'. Hit maximum number ({MaxNumberOfTimeoutsBeforeRecycle}) of timeouts. Terminating the process to mitigate potential deadlock.";
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
