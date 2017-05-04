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

namespace DurableTask.ServiceFabric
{
    using System;
    using System.Threading.Tasks;

    static class RetryHelper
    {
        public static Task ExecuteWithRetryOnTransient(Func<Task> action)
        {
            return ExecuteWithRetryOnTransient(action, CountBasedFixedDelayRetryPolicy.GetNewDefaultPolicy());
        }

        public static async Task ExecuteWithRetryOnTransient(Func<Task> action, RetryPolicy retryPolicy)
        {
            Exception lastException = null;

            while (retryPolicy.ShouldExecute())
            {
                try
                {
                    await action();
                    return;
                }
                catch (TimeoutException e)
                {
                    lastException = e;
                    await Task.Delay(retryPolicy.GetNextDelay());
                }
            }

            throw lastException;
        }

        public static Task<TResult> ExecuteWithRetryOnTransient<TResult>(Func<Task<TResult>> action)
        {
            return ExecuteWithRetryOnTransient(action, CountBasedFixedDelayRetryPolicy.GetNewDefaultPolicy());
        }

        public static async Task<TResult> ExecuteWithRetryOnTransient<TResult>(Func<Task<TResult>> action, RetryPolicy retryPolicy)
        {
            Exception lastException = null;

            while (retryPolicy.ShouldExecute())
            {
                try
                {
                    return await action();
                }
                catch (TimeoutException e)
                {
                    lastException = e;
                    await Task.Delay(retryPolicy.GetNextDelay());
                }
            }

            throw lastException;
        }
    }
}
