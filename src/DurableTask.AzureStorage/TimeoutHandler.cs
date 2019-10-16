using System;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.AzureStorage
{
    // Class that acts as a timeout handler to wrap Azure Storage calls, mitigating a deadlock that occurs with Azure Storage SDK 9.3.3.
    // The TimeoutHandler class is based off of the similar Azure Functions fix seen here: https://github.com/Azure/azure-webjobs-sdk/pull/2291
    internal static class TimeoutHandler
    {
        private static readonly TimeSpan DefaultTimeout = TimeSpan.FromMinutes(2);

        public static async Task<T> ExecuteWithTimeout<T>(string operationName, string clientRequestId, string account, string taskHub, Func<Task<T>> operation)
        {
            using (var cts = new CancellationTokenSource())
            {
                Task timeoutTask = Task.Delay(DefaultTimeout, cts.Token);
                Task<T> operationTask = operation();

                Task completedTask = await Task.WhenAny(timeoutTask, operationTask);

                if (Equals(timeoutTask, completedTask))
                {
                    var message = $"The operation '{operationName}' with id '{clientRequestId}' did not complete in '{DefaultTimeout}'.";
                    AnalyticsEventSource.Log.OrchestrationProcessingFailure(account, taskHub, null, null, message, Utils.ExtensionVersion);
                    Environment.FailFast(message);

                    return default(T);
                }
                
                cts.Cancel();

                return await operationTask;
            }
        }
    }
}
