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

            MethodInfo methodInfo = this.interfaze.GetMethod(binder.Name) ?? throw new InvalidOperationException("Method info not found.");
            Type[] genericArgumentValues = Utils.GetGenericMethodArguments(binder, methodInfo);

            List<object> arguments = new(args ?? Array.Empty<object>());

            foreach (Type typeArg in genericArgumentValues)
            {
                arguments.Add(new Utils.TypeMetadata() { AssemblyName = typeArg.Assembly.FullName, FullyQualifiedTypeName = typeArg.FullName });
            }

            args = arguments.ToArray();

            string normalizedMethodName = this.useFullyQualifiedMethodNames
                ? NameVersionHelper.GetFullyQualifiedMethodName(this.interfaze.Name, NameVersionHelper.GetDefaultName(binder))
                : NameVersionHelper.GetDefaultName(binder);

            if (returnType == typeof(Task))
            {
                result = this.context.ScheduleTask<object>(normalizedMethodName, NameVersionHelper.GetDefaultVersion(binder), args);
                return true;
            }

            returnType = Utils.GetGenericReturnType(methodInfo, genericArgumentValues);

            MethodInfo scheduleMethod = typeof(OrchestrationContext).GetMethod(
                "ScheduleTask",
                new[] { typeof(string), typeof(string), typeof(object[]) }) ??
                throw new Exception($"Method 'ScheduleTask' not found. Type Name: {nameof(OrchestrationContext)}");

            MethodInfo genericScheduleMethod = scheduleMethod.MakeGenericMethod(returnType);

            result = genericScheduleMethod.Invoke(this.context, new object[]
            {
                normalizedMethodName,
                NameVersionHelper.GetDefaultVersion(binder),
                args,
            });

            return true;
        }
    }
}