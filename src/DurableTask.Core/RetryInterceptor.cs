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
namespace DurableTask.Core
{
    using System;
    using System.Diagnostics;
    using System.Runtime.ExceptionServices;
    using System.Threading.Tasks;
    using DurableTask.Core.Common;
    using DurableTask.Core.Exceptions;
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
        /// <param name="context">The orchestration context of the function call</param>
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
        public async Task<T?> Invoke()
        {
            Exception? lastException = null;
            DateTime firstAttempt = this.context.CurrentUtcDateTime;

            for (var retryCount = 0; retryCount < this.retryOptions.MaxNumberOfAttempts; retryCount++)
            {
                try
                {
                    return await this.retryCall();
                }
                catch (Exception e) when (!Utils.IsFatal(e))
                {
                    if (e is OrchestrationException oe && oe.FailureDetails?.IsNonRetriable == true)
                    {
                        throw;
                    }

                    lastException = e;
                }

                bool isLastRetry = retryCount + 1 == this.retryOptions.MaxNumberOfAttempts;
                if (isLastRetry)
                {
                    // Earlier versions of this retry interceptor had a bug that scheduled an extra delay timer.
                    // It's unfortunately not possible to remove the extra timer since that would potentially
                    // break the history replay for existing orchestrations. Instead, we do the next best thing
                    // and schedule a timer that fires immediately instead of waiting for a full delay interval.
                    await this.context.CreateTimer(this.context.CurrentUtcDateTime, "Dummy timer for back-compat");
                    break;
                }

                TimeSpan nextDelay = ComputeNextDelay(retryCount, firstAttempt, lastException);
                if (nextDelay == TimeSpan.Zero)
                {
                    break;
                }

                DateTime retryAt = this.context.CurrentUtcDateTime.Add(nextDelay);
                await this.context.CreateTimer(retryAt, "Retry Attempt " + retryCount + 1);
            }

            if (lastException != null)
            {
                ExceptionDispatchInfo.Capture(lastException).Throw();
                throw lastException; // no op
            }

            return default;
        }

        TimeSpan ComputeNextDelay(int attempt, DateTime firstAttempt, Exception failure)
        {
            TimeSpan nextDelay = TimeSpan.Zero;
            try
            {
                if (this.retryOptions.Handle(failure))
                {
                    DateTime retryExpiration = (this.retryOptions.RetryTimeout != TimeSpan.MaxValue)
                        ? firstAttempt.Add(this.retryOptions.RetryTimeout)
                        : DateTime.MaxValue;
                    if (this.context.CurrentUtcDateTime < retryExpiration)
                    {
                        double nextDelayInMilliseconds = this.retryOptions.FirstRetryInterval.TotalMilliseconds *
                                                         Math.Pow(this.retryOptions.BackoffCoefficient, attempt);
                        nextDelay = nextDelayInMilliseconds < this.retryOptions.MaxRetryInterval.TotalMilliseconds
                            ? TimeSpan.FromMilliseconds(nextDelayInMilliseconds)
                            : this.retryOptions.MaxRetryInterval;
                    }
                }
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                // Catch any exceptions during ComputeNextDelay so we don't override original error with new error
                TraceHelper.TraceExceptionInstance(TraceEventType.Error, "RetryInterceptor-ComputeNextDelayException", this.context.OrchestrationInstance, e);
            }

            return nextDelay;
        }
    }
}