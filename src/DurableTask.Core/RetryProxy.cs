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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Dynamic;
    using System.Linq;
    using System.Reflection;
    using System.Threading.Tasks;

    internal class RetryProxy<T> : DynamicObject
    {
        readonly OrchestrationContext context;
        readonly RetryOptions retryOptions;
        readonly IDictionary<string, Type> returnTypes;
        readonly T wrappedObject;

        public RetryProxy(OrchestrationContext context, RetryOptions retryOptions, T wrappedObject)
        {
            this.context = context;
            this.retryOptions = retryOptions;
            this.wrappedObject = wrappedObject;

            //If type can be determined by name
            this.returnTypes = typeof(T).GetMethods()
                .Where(method => !method.IsSpecialName)
                .GroupBy(method => method.Name)
                .Where(group => group.Select(method => method.ReturnType).Distinct().Count() == 1)
                .Select(group => new
                {
                    Name = group.Key,
                    ReturnType = group.Select(method => method.ReturnType).Distinct().Single()
                })
                .ToDictionary(info => info.Name, info => info.ReturnType);
        }

        public override bool TryInvokeMember(InvokeMemberBinder binder, object[] args, out object result)
        {
            if (!this.returnTypes.TryGetValue(binder.Name, out Type returnType))
            {
                throw new Exception("Method name '" + binder.Name + "' not known.");
            }

            if (returnType == typeof(Task))
            {
                result = InvokeWithRetry<object>(binder.Name, args);
            }
            else
            {
                if (!returnType.IsGenericType)
                {
                    throw new Exception("Return type is not a generic type. Type Name: " + returnType.FullName);
                }

                Type[] genericArguments = returnType.GetGenericArguments();
                if (genericArguments.Length != 1)
                {
                    throw new Exception("Generic Parameters are not equal to 1. Type Name: " + returnType.FullName);
                }

                MethodInfo invokeMethod = GetType().GetMethod("InvokeWithRetry");

                Debug.Assert(invokeMethod != null);

                MethodInfo genericInvokeMethod = invokeMethod.MakeGenericMethod(genericArguments[0]);
                result = genericInvokeMethod.Invoke(this, new object[] { binder.Name, args });
            }

            return true;
        }

        public async Task<TReturnType> InvokeWithRetry<TReturnType>(string methodName, object[] args)
        {
            Task<TReturnType> RetryCall()
            {
#if NETSTANDARD2_0
                return Dynamitey.Dynamic.InvokeMember(this.wrappedObject, methodName, args);
#else
                return ImpromptuInterface.Impromptu.InvokeMember(this.wrappedObject, methodName, args);
#endif
            }

            var retryInterceptor = new RetryInterceptor<TReturnType>(this.context, this.retryOptions, RetryCall);

            return await retryInterceptor.Invoke();
        }
    }
}