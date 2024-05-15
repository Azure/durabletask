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

    /// <summary>
    /// Helper class for getting name information from types and instances
    /// </summary>
    public static class NameVersionHelper
    {
        internal static string GetDefaultMethodName(MethodInfo methodInfo, bool useFullyQualifiedMethodNames)
        {
            string methodName = methodInfo.Name;
            if (useFullyQualifiedMethodNames && methodInfo.DeclaringType != null)
            {
                methodName = GetFullyQualifiedMethodName(methodInfo.DeclaringType.Name, methodInfo.Name);
            }

            return methodName;
        }

        /// <summary>
        /// Gets the default name of an Object instance without using fully qualified names
        /// </summary>
        /// <param name="obj">Object to get the name for</param>
        /// <returns>Name of the object instance's type</returns>
        public static string GetDefaultName(object obj)
        {
            return GetDefaultName(obj, false);
        }

        /// <summary>
        /// Gets the default name of an Object instance using reflection
        /// </summary>
        /// <param name="obj">Object to get the name for</param>
        /// <param name="useFullyQualifiedMethodNames">Boolean indicating whether to use fully qualified names or not</param>
        /// <returns>Name of the object instance's type</returns>
        public static string GetDefaultName(object obj, bool useFullyQualifiedMethodNames)
        {
            if (obj == null)
            {
                throw new ArgumentNullException(nameof(obj));
            }

            string name;
            Type type;
            MethodInfo methodInfo;
            InvokeMemberBinder binder;
            if ((type = obj as Type) != null)
            {
                name = type.ToString();
            }
            else if ((methodInfo = obj as MethodInfo) != null)
            {
                name = GetDefaultMethodName(methodInfo, useFullyQualifiedMethodNames);
            }
            else if ((binder = obj as InvokeMemberBinder) != null)
            {
                name = binder.Name;
            }
            else
            {
                name = obj.GetType().ToString();
            }

            return name;
        }

        /// <summary>
        /// Gets the default version for an object instance's type
        /// </summary>
        /// <param name="obj">Object to get the version for</param>
        /// <returns>The version as string</returns>
        public static string GetDefaultVersion(object obj)
        {
            return string.Empty;
        }

        internal static string GetFullyQualifiedMethodName(string declaringType, string methodName)
        {
            if (string.IsNullOrWhiteSpace(declaringType))
            {
                return methodName;
            }

            return declaringType + "." + methodName;
        }

        /// <summary>
        /// Gets the fully qualified method name by joining a prefix representing the declaring type and a suffix representing the parameter list.
        /// For example,
        /// "DurableTask.Emulator.Tests.EmulatorFunctionalTests+IInheritedTestOrchestrationTasksB`2[System.Int32,System.String].Juggle.(Int32,Boolean)"
        /// would be the result for the method `Juggle(int, bool)` as member of
        /// generic type interface declared like `DurableTask.Emulator.Tests.EmulatorFunctionalTests.IInheritedTestOrchestrationTasksB{int, string}`,
        /// even if the method were inherited from a base interface.
        /// </summary>
        /// <param name="declaringType">typically the result of call to Type.ToString(): Type.FullName is more verbose</param>
        /// <param name="methodInfo"></param>
        /// <returns></returns>
        internal static string GetFullyQualifiedMethodName(string declaringType, MethodInfo methodInfo)
        {
            IEnumerable<string> paramTypeNames = methodInfo.GetParameters().Select(x => x.ParameterType.Name);
            string paramTypeNamesCsv = string.Join(",", paramTypeNames);
            string methodNameWithParameterList = $"{methodInfo.Name}.({paramTypeNamesCsv})";
            return GetFullyQualifiedMethodName(declaringType, methodNameWithParameterList);
        }

        /// <summary>
        /// Gets all methods from an interface, including those inherited from a base interface
        /// </summary>
        /// <param name="t"></param>
        /// <param name="getMethodUniqueId"></param>
        /// <param name="visited"></param>
        /// <returns></returns>
        internal static IList<MethodInfo> GetAllInterfaceMethods(Type t, Func<MethodInfo, string> getMethodUniqueId, HashSet<string> visited = null)
        {
            if (visited == null)
            {
                visited = new HashSet<string>();
            }
            List<MethodInfo> result = new List<MethodInfo>();
            foreach (MethodInfo m in t.GetMethods())
            {
                string name = getMethodUniqueId(m);
                if (!visited.Contains(name))
                {
                    // In some cases, such as when a generic type interface inherits an interface with the same name, Task.GetMethod includes the methods from the base interface.
                    // This check is to avoid dupicates from these.
                    result.Add(m);
                    visited.Add(name);
                }
            }
            foreach (Type baseInterface in t.GetInterfaces())
            {
                result.AddRange(GetAllInterfaceMethods(baseInterface, getMethodUniqueId, visited: visited));
            }
            return result;
        }
    }
}