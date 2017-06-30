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

namespace DurableTask.Core
{
    using System;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using DurableTask.Core.Common;
    using DurableTask.Core.Tracing;

    /// <summary>
    /// Generic retry class to handle retries on a function call with specified retry options
    /// </summary>
    /// <typeparam name="T">Type to return from the called Func</typeparam>
    public class RetryInterceptor<T>
    {
        readonly OrchestrationContext context;
        readonly Func<Task<T>> retryCall;
        readonly RetryOptions retryOptions;

        /// <summary>
        /// Creates a new instance of the RetryInterceptor with specified parameters
        /// </summary>
        /// <param name="context">The orchestraion context of the function call</param>
        /// <param name="retryOptions">The options for performing retries</param>
        /// <param name="retryCall">The code to execute</param>
        public RetryInterceptor(OrchestrationContext context, RetryOptions retryOptions, Func<Task<T>> retryCall)
        {
            this.context = context;
            this.retryOptions = retryOptions;
            this.retryCall = retryCall;
        }

        /// <summary>
        /// Invokes the method/code to call and retries on exception based on the retry options
        /// </summary>
        /// <returns>The return value of the supplied retry call</returns>
        /// <exception cref="Exception">The final exception encountered if the call did not succeed</exception>
        public async Task<T> Invoke()
        {
            Exception lastException = null;
            DateTime firstAttempt = context.CurrentUtcDateTime;
            for (int retryCount = 0; retryCount < retryOptions.MaxNumberOfAttempts; retryCount++)
            {
                try
                {
                    return await retryCall();
                }
                catch (Exception e) when (!Utils.IsFatal(e))
                {
                    lastException = e;
                }

                TimeSpan nextDelay = ComputeNextDelay(retryCount, firstAttempt, lastException);
                if (nextDelay == TimeSpan.Zero) break;
                DateTime retryAt = context.CurrentUtcDateTime.Add(nextDelay);
                await context.CreateTimer(retryAt, "Retry Attempt " + retryCount + 1);
            }

            throw lastException;
        }

        TimeSpan ComputeNextDelay(int attempt, DateTime firstAttempt, Exception failure)
        {
            TimeSpan nextDelay = TimeSpan.Zero;
            try
            {
                if (retryOptions.Handle(failure))
                {
                    DateTime retryExpiration = (retryOptions.RetryTimeout != TimeSpan.MaxValue)
                        ? firstAttempt.Add(retryOptions.RetryTimeout)
                        : DateTime.MaxValue;
                    if (context.CurrentUtcDateTime < retryExpiration)
                    {
                        double nextDelayInMilliSeconds = retryOptions.FirstRetryInterval.TotalMilliseconds*
                                                         Math.Pow(retryOptions.BackoffCoefficient, attempt);
                        nextDelay = (nextDelayInMilliSeconds < retryOptions.MaxRetryInterval.TotalMilliseconds)
                            ? TimeSpan.FromMilliseconds(nextDelayInMilliSeconds)
                            : retryOptions.MaxRetryInterval;
                    }
                }
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                // Catch any exceptions during ComputeNextDelay so we don't override original error with new error
                TraceHelper.TraceExceptionInstance(TraceEventType.Error, "RetryInterceptor-ComputeNextDelayException", context.OrchestrationInstance, e);
            }

            return nextDelay;
        }
    }
}