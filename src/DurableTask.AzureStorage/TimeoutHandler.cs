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
    using System;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    // Class that acts as a timeout handler to wrap Azure Storage calls, mitigating a deadlock that occurs with Azure Storage SDK 9.3.3.
    // The TimeoutHandler class is based off of the similar Azure Functions fix seen here: https://github.com/Azure/azure-webjobs-sdk/pull/2291
    internal static class TimeoutHandler
    {
        private static readonly TimeSpan DefaultTimeout = TimeSpan.FromMinutes(2);

        public static async Task<T> ExecuteWithTimeout<T>(
            string operationName,
            string clientRequestId,
            string account,
            AzureStorageOrchestrationServiceSettings settings,
            Func<Task<T>> operation)
        {
            if (Debugger.IsAttached)
            {
                // ignore long delays while debugging
                return await operation();
            }

            using (var cts = new CancellationTokenSource())
            {
                Task timeoutTask = Task.Delay(DefaultTimeout, cts.Token);
                Task<T> operationTask = operation();

                Task completedTask = await Task.WhenAny(timeoutTask, operationTask);

                if (Equals(timeoutTask, completedTask))
                {
                    string taskHubName = settings.TaskHubName;
                    string message = $"The operation '{operationName}' with id '{clientRequestId}' did not complete in '{DefaultTimeout}'. Terminating the process to mitigate potential deadlock.";
                    settings.Logger.GeneralError(account ?? "", taskHubName ?? "", message);

                    // Delay to ensure the ETW event gets written
                    await Task.Delay(TimeSpan.FromSeconds(3));
                    Environment.FailFast(message);

                    return default(T);
                }
                
                cts.Cancel();

                return await operationTask;
            }
        }
    }
}
