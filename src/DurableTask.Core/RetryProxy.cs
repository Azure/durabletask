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
    using DurableTask.Core.Common;
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

            MethodInfo methodInfo = typeof(T).GetMethod(binder.Name) ?? throw new InvalidOperationException("Method info not found.");

            Type[] genericArgumentValues = Utils.GetGenericMethodArguments(binder, methodInfo);

            if (returnType == typeof(Task))
            {
                result = this.InvokeWithRetry<object>(binder.Name, genericArgumentValues, args);
                return true;
            }

            returnType = Utils.GetGenericReturnType(methodInfo, genericArgumentValues);

            MethodInfo invokeMethod = this.GetType().GetMethod("InvokeWithRetry", BindingFlags.Instance | BindingFlags.NonPublic);

            Debug.Assert(invokeMethod != null, "null");

            MethodInfo genericInvokeMethod = invokeMethod.MakeGenericMethod(returnType);
            result = genericInvokeMethod.Invoke(this, new object[] { binder.Name, genericArgumentValues, args });

            return true;
        }

        private async Task<TReturnType> InvokeWithRetry<TReturnType>(string methodName, Type[] genericArgs, object[] args)
        {
            Task<TReturnType> RetryCall()
            {
#if NETSTANDARD2_0
                return Dynamitey.Dynamic.InvokeMember(this.wrappedObject, new Dynamitey.InvokeMemberName(methodName, genericArgs), methodName, args);
#else
                return ImpromptuInterface.Impromptu.InvokeMember(this.wrappedObject, new ImpromptuInterface.InvokeMemberName(methodName, genericArgs), args);
#endif
            }

            var retryInterceptor = new RetryInterceptor<TReturnType>(this.context, this.retryOptions, RetryCall);

            return await retryInterceptor.Invoke();
        }
    }
}