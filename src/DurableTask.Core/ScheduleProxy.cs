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

    internal class ScheduleProxy : DynamicObject
    {
        readonly OrchestrationContext context;
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
            interfaze = @interface;
            this.useFullyQualifiedMethodNames = useFullyQualifiedMethodNames;

            //If type can be determined by name
            returnTypes = interfaze.GetMethods()
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
            Type returnType = null;
            if (!returnTypes.TryGetValue(binder.Name, out returnType))
            {
                throw new Exception("Method name '" + binder.Name + "' not known.");
            }

            string normalizedMethodName = useFullyQualifiedMethodNames
                ? NameVersionHelper.GetFullyQualifiedMethodName(interfaze.Name, NameVersionHelper.GetDefaultName(binder))
                : NameVersionHelper.GetDefaultName(binder);

            if (returnType.Equals(typeof (Task)))
            {
                result = context.ScheduleTask<object>(normalizedMethodName,
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

                MethodInfo scheduleMethod = typeof (OrchestrationContext).GetMethod("ScheduleTask",
                    new[] {typeof (string), typeof (string), typeof (object[])});
                MethodInfo genericScheduleMethod = scheduleMethod.MakeGenericMethod(genericArguments[0]);

                result = genericScheduleMethod.Invoke(context, new object[]
                {
                    normalizedMethodName,
                    NameVersionHelper.GetDefaultVersion(binder), args
                });
            }

            return true;
        }
    }
}