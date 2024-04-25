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
    using System.Linq;
    using System.Reflection;
    using System.Threading.Tasks;
    using Castle.DynamicProxy;
    using DurableTask.Core.Common;

    /// <remarks>
    ///     This is deprecated and exists only for back-compatibility.
    ///     See <see cref="ScheduleProxyV2"/>, which adds support for C# interface features such as inheritance, generics, and method overloading.
    /// </remarks>
    internal class ScheduleProxy : IInterceptor
    {
        private readonly OrchestrationContext context;
        private readonly bool useFullyQualifiedMethodNames;

        /// <summary>
        /// Initializes a new instance of the <see cref="ScheduleProxy"/> class.
        /// </summary>
        /// <param name="context">The orchestration context.</param>
        public ScheduleProxy(OrchestrationContext context)
            : this(context, false)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ScheduleProxy"/> class.
        /// </summary>
        /// <param name="context">The orchestration context.</param>
        /// <param name="useFullyQualifiedMethodNames">A flag indicating whether to use fully qualified method names.</param>
        public ScheduleProxy(OrchestrationContext context, bool useFullyQualifiedMethodNames)
        {
            this.context = context;
            this.useFullyQualifiedMethodNames = useFullyQualifiedMethodNames;
        }

        /// <inheritdoc/>
        public void Intercept(IInvocation invocation)
        {
            Type returnType = invocation.Method.ReturnType;

            if (!typeof(Task).IsAssignableFrom(returnType))
            {
                throw new InvalidOperationException($"Invoked method must return a task. Current return type is {invocation.Method.ReturnType}");
            }

            Type[] genericArgumentValues = invocation.GenericArguments ?? Array.Empty<Type>();
            List<object> arguments = new(invocation.Arguments);

            foreach (var typeArg in genericArgumentValues)
            {
                arguments.Add(new Utils.TypeMetadata { AssemblyName = typeArg.Assembly.FullName!, FullyQualifiedTypeName = typeArg.FullName });
            }

            string normalizedMethodName = this.NormalizeMethodName(invocation.Method);

            if (returnType == typeof(Task))
            {
                invocation.ReturnValue = this.context.ScheduleTask<object>(
                    normalizedMethodName,
                    NameVersionHelper.GetDefaultVersion(invocation.Method),
                    arguments.ToArray());
                return;
            }

            returnType = invocation.Method.ReturnType.GetGenericArguments().Single();

            MethodInfo scheduleMethod = typeof(OrchestrationContext).GetMethod(
                "ScheduleTask",
                new[] { typeof(string), typeof(string), typeof(object[]) }) ??
                throw new Exception($"Method 'ScheduleTask' not found. Type Name: {nameof(OrchestrationContext)}");

            MethodInfo genericScheduleMethod = scheduleMethod.MakeGenericMethod(returnType);

            invocation.ReturnValue = genericScheduleMethod.Invoke(this.context, new object[]
            {
                normalizedMethodName,
                NameVersionHelper.GetDefaultVersion(invocation.Method),
                arguments.ToArray(),
            });

            return;
        }

        protected virtual string NormalizeMethodName(MethodInfo method)
        {
            return NameVersionHelper.GetDefaultName(method, this.useFullyQualifiedMethodNames);
        }
    }
}