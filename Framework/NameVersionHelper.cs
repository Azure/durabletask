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

namespace DurableTask
{
    using System;
    using System.Dynamic;
    using System.Reflection;

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

        public static string GetDefaultName(object obj)
        {
            return GetDefaultName(obj, false);
        }

        public static string GetDefaultName(object obj, bool useFullyQualifiedMethodNames)
        {
            if (obj == null)
            {
                throw new ArgumentNullException("obj");
            }

            string name;
            Type type = null;
            MethodInfo methodInfo = null;
            InvokeMemberBinder binder = null;
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

        public static string GetDefaultVersion(object obj)
        {
            return string.Empty;
        }

        internal static string GetFullyQualifiedMethodName(string declaringType, string methodName)
        {
            if (string.IsNullOrEmpty(declaringType))
            {
                return methodName;
            }
            return declaringType + "." + methodName;
        }
    }
}