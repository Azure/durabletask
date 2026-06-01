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

namespace DurableTask.AzureServiceFabric.Service
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using DurableTask.AzureServiceFabric.Models;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.Core.Tracing;
    using Newtonsoft.Json.Serialization;

    /// <summary>
    /// A serialization binder that restricts deserialization to only the specific types
    /// that flow through the Service Fabric proxy HTTP endpoints. This prevents untrusted
    /// <c>$type</c> metadata in JSON payloads from loading arbitrary assemblies or
    /// instantiating arbitrary types.
    /// </summary>
    public sealed class AllowedTypesSerializationBinder : ISerializationBinder
    {
        static readonly HashSet<Type> AllowedTypes = BuildAllowedTypes();

        readonly DefaultSerializationBinder defaultBinder = new DefaultSerializationBinder();
        readonly ConcurrentDictionary<Type, bool> typeAllowCache = new ConcurrentDictionary<Type, bool>();

        /// <inheritdoc />
        public Type BindToType(string assemblyName, string typeName)
        {
            // Validate assembly name before resolving to prevent Assembly.Load of untrusted assemblies.
            if (!string.IsNullOrWhiteSpace(assemblyName) && !IsAssemblyNameAllowed(assemblyName))
            {
                throw new InvalidOperationException(
                    $"Deserialization of type '{typeName}' from assembly '{assemblyName}' is not allowed. " +
                    $"Only known DurableTask proxy endpoint types are permitted.");
            }

            Type resolvedType = this.defaultBinder.BindToType(assemblyName, typeName);

            if (resolvedType == null || !IsTypeAllowed(resolvedType))
            {
                throw new InvalidOperationException(
                    $"Deserialization of type '{typeName}' from assembly '{assemblyName}' is not allowed. " +
                    $"Only known DurableTask proxy endpoint types are permitted.");
            }

            return resolvedType;
        }

        /// <inheritdoc />
        public void BindToName(Type serializedType, out string assemblyName, out string typeName)
        {
            this.defaultBinder.BindToName(serializedType, out assemblyName, out typeName);
        }

        static HashSet<Type> BuildAllowedTypes()
        {
            var types = new HashSet<Type>
            {
                // Core domain types that appear in the proxy endpoint object graph
                typeof(TaskMessage),
                typeof(OrchestrationInstance),
                typeof(OrchestrationExecutionContext),
                typeof(OrchestrationState),
                typeof(ParentInstance),
                typeof(DistributedTraceContext),
                typeof(FailureDetails),

                // Service Fabric proxy parameter types
                typeof(CreateTaskOrchestrationParameters),
                typeof(PurgeOrchestrationHistoryParameters),

                // System collections used for Tags and FailureDetails.Properties
                typeof(Dictionary<string, string>),
                typeof(Dictionary<string, object>),
            };

            // All HistoryEvent subclasses (self-maintaining via assembly scanning).
            // Use the same logic as HistoryEvent.KnownTypes() but handle partial load failures
            // that can occur when not all referenced assemblies are available.
            try
            {
                foreach (Type historyEventType in HistoryEvent.KnownTypes())
                {
                    types.Add(historyEventType);
                }
            }
            catch (ReflectionTypeLoadException ex)
            {
                foreach (Type loadedType in ex.Types.Where(t => t != null && !t.IsAbstract && typeof(HistoryEvent).IsAssignableFrom(t)))
                {
                    types.Add(loadedType);
                }
            }

            return types;
        }

        static bool IsAssemblyNameAllowed(string assemblyName)
        {
            // Strip version/culture/publicKeyToken if present
            int commaIndex = assemblyName.IndexOf(',');
            string shortName = commaIndex >= 0 ? assemblyName.Substring(0, commaIndex).Trim() : assemblyName.Trim();

            return string.Equals(shortName, typeof(TaskMessage).Assembly.GetName().Name, StringComparison.OrdinalIgnoreCase)
                || string.Equals(shortName, typeof(FabricOrchestrationProvider).Assembly.GetName().Name, StringComparison.OrdinalIgnoreCase)
                || string.Equals(shortName, "mscorlib", StringComparison.OrdinalIgnoreCase)
                || string.Equals(shortName, "System.Private.CoreLib", StringComparison.OrdinalIgnoreCase);
        }

        bool IsTypeAllowed(Type type)
        {
            return this.typeAllowCache.GetOrAdd(type, t =>
            {
                // Primitives and strings are always safe
                if (t.IsPrimitive || t == typeof(string) || t == typeof(DateTime)
                    || t == typeof(DateTimeOffset) || t == typeof(TimeSpan)
                    || t == typeof(Guid) || t == typeof(decimal))
                {
                    return true;
                }

                // Arrays of allowed types
                if (t.IsArray)
                {
                    return IsTypeAllowed(t.GetElementType());
                }

                // Nullable<T> of allowed types
                Type nullable = Nullable.GetUnderlyingType(t);
                if (nullable != null)
                {
                    return IsTypeAllowed(nullable);
                }

                // Enums are safe (serialize as values)
                if (t.IsEnum)
                {
                    return true;
                }

                return AllowedTypes.Contains(t);
            });
        }
    }
}
