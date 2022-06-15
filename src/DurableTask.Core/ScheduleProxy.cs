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
    using System.Dynamic;
    using System.Linq;
    using System.Reflection;
    using System.Threading.Tasks;
    using DurableTask.Core.Common;

    internal class ScheduleProxy : DynamicObject
    {
        readonly OrchestrationContext context;

        // ReSharper disable once IdentifierTypo (intentional)
        readonly Type interfaze;
        readonly IDictionary<string, Type> returnTypes;
        readonly bool useFullyQualifiedMethodNames;

        public ScheduleProxy(OrchestrationContext context, Type @interface)
            : this(context, @interface, false)
        {
        }

        public ScheduleProxy(OrchestrationContext context, Type @interface, bool useFullyQualifiedMethodNames)
        {
            this.context = context;
            this.interfaze = @interface;
            this.useFullyQualifiedMethodNames = useFullyQualifiedMethodNames;

            //If type can be determined by name
            this.returnTypes = this.interfaze.GetMethods()
                .Where(method => !method.IsSpecialName)
                .GroupBy(method => NameVersionHelper.GetDefaultName(method))
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

            var binderType = binder.GetType();

            MethodInfo methodInfo = this.interfaze.GetMethod(binder.Name);

            if (methodInfo.IsGenericMethod && !string.Equals(binderType.Name, "CSharpInvokeMemberBinder"))
            {
                throw new InvalidOperationException("Generic method invoked but method binder is not C#");
            }

            Type[] typeArguments = binderType.GetProperty("TypeArguments").GetValue(binder) as Type[];
            typeArguments = typeArguments == null ? Array.Empty<Type>() : typeArguments;

            if (typeArguments.Length != methodInfo.GetGenericArguments().Length)
            {
                throw new InvalidOperationException("Generic method mismatch");
            }

            // Append the type arguments at the end of the args array.
            List<object> arguments = new(args);

            foreach (var typeArg in typeArguments)
            {
                arguments.Add(new Utils.TypeMetadata()
                {
                    AssemblyName = typeArg.Assembly.FullName,
                    FullyQualifiedTypeName = typeArg.FullName,
                });
            }

            args = arguments.ToArray();

            string normalizedMethodName = this.useFullyQualifiedMethodNames
                ? NameVersionHelper.GetFullyQualifiedMethodName(this.interfaze.Name, NameVersionHelper.GetDefaultName(binder))
                : NameVersionHelper.GetDefaultName(binder);

            if (returnType == typeof(Task))
            {
                result = this.context.ScheduleTask<object>(normalizedMethodName,
                    NameVersionHelper.GetDefaultVersion(binder), args);
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

                var genericArgument = genericArguments.Single();

                if (genericArgument.IsGenericParameter)
                {
                    var index = Array.IndexOf(methodInfo.GetGenericArguments(), genericArgument);

                    // Change the type from its generic representation to the type passed in on invocation.
                    genericArgument = typeArguments[index];
                }

                MethodInfo scheduleMethod = typeof(OrchestrationContext).GetMethod("ScheduleTask",
                    new[] { typeof(string), typeof(string), typeof(object[]) });
                if (scheduleMethod == null)
                {
                    throw new Exception($"Method 'ScheduleTask' not found. Type Name: {nameof(OrchestrationContext)}");
                }

                MethodInfo genericScheduleMethod = scheduleMethod.MakeGenericMethod(genericArgument);

                result = genericScheduleMethod.Invoke(this.context, new object[]
                {
                    normalizedMethodName,
                    NameVersionHelper.GetDefaultVersion(binder), args
                });
            }

            return true;
        }
    }
}