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

namespace DurableTask.Core.Common
{
    using Microsoft.Win32.SafeHandles;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Threading;

    internal static class TypeExtension
    {
        internal static void HandleTypeValidationError(string metadataInfo)
        {
            var isCancellationTokenWarningOnly = true;

            if (isCancellationTokenWarningOnly)
            {
                throw new InvalidOperationException($"Dangerous type deserialization found while converting method to task-activity for {metadataInfo}.");
            }
        }

        /// <summary>
        /// Checks if a type has any dangerous object which can conflict with native OS event handle.
        /// E.g., CancellationToken, etc.
        /// </summary>
        /// <param name="typeContaining"></param>
        /// <returns></returns>
        public static bool IsEqualOrContainsNativeType(this Type typeContaining)
        {
            return typeContaining.IsEqualOrContainsType(typeof(WaitHandle))
                || typeContaining.IsEqualOrContainsType(typeof(SafeWaitHandle));
        }

        public static bool IsEqualOrContainsType(this Type typeContaining, Type typeContained)
        {
            if (typeContaining == typeContained)
            {
                return true;
            }

            List<Type> processedTypes = new List<Type>();
            return typeContaining.ContainsType(typeContained, processedTypes);
        }

        private static bool ContainsType(this Type typeContaining, Type typeContained, List<Type> processedTypes)
        {
            if (processedTypes.Any(x => x == typeContaining))
            {
                // Self-reference, no point processing it again.
                return false;
            }
            else
            {
                processedTypes.Add(typeContaining);
            }

            // Get all properties and fields of typeT
            PropertyInfo[] properties = typeContaining.GetProperties();
            FieldInfo[] fields = typeContaining.GetFields();

            // Check properties
            foreach (var property in properties)
            {
                if (property.PropertyType == typeContained)
                    return true;
                else if (!property.PropertyType.IsPrimitive && property.PropertyType.ContainsType(typeContained, processedTypes))
                    return true;
            }

            // Check fields
            foreach (var field in fields)
            {
                if (field.FieldType == typeContained)
                    return true;
                else if (!field.FieldType.IsPrimitive && field.FieldType.ContainsType(typeContained, processedTypes))
                    return true;
            }

            return false;
        }
    }
}
