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

namespace DurableTask.AzureServiceFabric.TaskHelpers
{
    using System;
    using System.Diagnostics;
    using System.Threading.Tasks;

    using DurableTask.AzureServiceFabric.Tracing;

    static class RetryHelper
    {
        public static Task ExecuteWithRetryOnTransient(Func<Task> action, string uniqueActionIdentifier)
        {
            return ExecuteWithRetryOnTransient(action, CountBasedFixedDelayRetryPolicy.GetNewDefaultPolicy(), uniqueActionIdentifier);
        }

        static async Task ExecuteWithRetryOnTransient(Func<Task> action, RetryPolicy retryPolicy, string uniqueActionIdentifier)
        {
            Exception lastException = null;

            int attemptNumber = 0;
            while (retryPolicy.ShouldExecute())
            {
                attemptNumber++;
                Stopwatch timer = Stopwatch.StartNew();
                try
                {
                    await action();
                    timer.Stop();
                    ServiceFabricProviderEventSource.Tracing.LogMeasurement($"{uniqueActionIdentifier}, Attempt Number : {attemptNumber}, Result : Success", timer.ElapsedMilliseconds);
                    return;
                }
                catch (Exception e)
                {
                    timer.Stop();
                    lastException = e;
                    bool shouldRetry = ExceptionUtilities.IsRetryableFabricException(e);

                    ExceptionUtilities.LogReliableCollectionException(uniqueActionIdentifier, attemptNumber, e, shouldRetry);
                    ServiceFabricProviderEventSource.Tracing.LogMeasurement($"{uniqueActionIdentifier}, Attempt Number : {attemptNumber}, ShouldRetry : {shouldRetry}", timer.ElapsedMilliseconds);

                    if (shouldRetry)
                    {
                        await Task.Delay(retryPolicy.GetNextDelay());
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            throw lastException;
        }

        public static Task<TResult> ExecuteWithRetryOnTransient<TResult>(Func<Task<TResult>> action, string uniqueActionIdentifier)
        {
            return ExecuteWithRetryOnTransient(action, CountBasedFixedDelayRetryPolicy.GetNewDefaultPolicy(), uniqueActionIdentifier);
        }

        static async Task<TResult> ExecuteWithRetryOnTransient<TResult>(Func<Task<TResult>> action, RetryPolicy retryPolicy, string uniqueActionIdentifier)
        {
            Exception lastException = null;

            int attemptNumber = 0;
            while (retryPolicy.ShouldExecute())
            {
                attemptNumber++;
                Stopwatch timer = Stopwatch.StartNew();
                try
                {
                    var result = await action();
                    timer.Stop();
                    ServiceFabricProviderEventSource.Tracing.LogMeasurement($"{uniqueActionIdentifier}, Attempt Number : {attemptNumber}, Result : Success", timer.ElapsedMilliseconds);
                    return result;
                }
                catch (Exception e)
                {
                    timer.Stop();
                    lastException = e;
                    bool shouldRetry = ExceptionUtilities.IsRetryableFabricException(e);

                    ExceptionUtilities.LogReliableCollectionException(uniqueActionIdentifier, attemptNumber, e, shouldRetry);
                    ServiceFabricProviderEventSource.Tracing.LogMeasurement($"{uniqueActionIdentifier}, Attempt Number : {attemptNumber}, ShouldRetry : {shouldRetry}", timer.ElapsedMilliseconds);

                    if (shouldRetry)
                    {
                        await Task.Delay(retryPolicy.GetNextDelay());
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            throw lastException;
        }
    }
}
