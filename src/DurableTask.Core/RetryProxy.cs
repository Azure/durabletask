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
    using System.Linq;
    using System.Reflection;
    using System.Threading.Tasks;
    using Castle.DynamicProxy;

    internal class RetryProxy : IInterceptor
    {
        private readonly OrchestrationContext context;
        private readonly RetryOptions retryOptions;

        /// <summary>
        /// Initializes a new instance of the <see cref="RetryProxy"/> class.
        /// </summary>
        /// <param name="context">The orchestration context.</param>
        /// <param name="retryOptions">The retry options.</param>
        public RetryProxy(OrchestrationContext context, RetryOptions retryOptions)
        {
            this.context = context;
            this.retryOptions = retryOptions;
        }

        /// <inheritdoc/>
        public void Intercept(IInvocation invocation)
        {
            var returnType = invocation.Method.ReturnType;
            if (!typeof(Task).IsAssignableFrom(returnType))
            {
                throw new InvalidOperationException($"Invoked method must return a task. Current return type is {invocation.Method.ReturnType}");
            }

            if (returnType == typeof(Task))
            {
                invocation.ReturnValue = this.InvokeWithRetry<object>(invocation);
                return;
            }

            returnType = invocation.Method.ReturnType.GetGenericArguments().Single();

            MethodInfo invokeMethod = this.GetType().GetMethod("InvokeWithRetry", BindingFlags.Instance | BindingFlags.NonPublic);

            Debug.Assert(invokeMethod != null);

            MethodInfo genericInvokeMethod = invokeMethod.MakeGenericMethod(returnType);
            invocation.ReturnValue = genericInvokeMethod.Invoke(this, new object[] { invocation });

            return;
        }

        private async Task<TReturnType> InvokeWithRetry<TReturnType>(IInvocation invocation)
        {
            Task<TReturnType> RetryCall()
            {
                return (Task<TReturnType>)invocation.GetConcreteMethod().Invoke(invocation.InvocationTarget, invocation.Arguments);
            }

            var retryInterceptor = new RetryInterceptor<TReturnType>(this.context, this.retryOptions, RetryCall);

            return await retryInterceptor.Invoke();
        }
    }
}