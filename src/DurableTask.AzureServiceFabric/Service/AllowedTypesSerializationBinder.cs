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
    using Newtonsoft.Json.Serialization;

    /// <summary>
    /// A serialization binder that restricts deserialization to only known DurableTask types
    /// and core system types. This prevents untrusted <c>$type</c> metadata in JSON payloads
    /// from loading arbitrary assemblies or instantiating arbitrary types.
    /// </summary>
    public sealed class AllowedTypesSerializationBinder : ISerializationBinder
    {
        static readonly HashSet<string> AllowedAssemblyNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            typeof(Core.TaskMessage).Assembly.GetName().Name,           // DurableTask.Core
            typeof(FabricOrchestrationProvider).Assembly.GetName().Name, // DurableTask.AzureServiceFabric
            "mscorlib",                                                  // .NET Framework core types
            "System.Private.CoreLib",                                    // .NET Core/5+ core types
        };

        readonly DefaultSerializationBinder defaultBinder = new DefaultSerializationBinder();
        readonly ConcurrentDictionary<string, bool> assemblyAllowCache = new ConcurrentDictionary<string, bool>(StringComparer.OrdinalIgnoreCase);

        /// <inheritdoc />
        public Type BindToType(string assemblyName, string typeName)
        {
            if (!IsAssemblyAllowed(assemblyName))
            {
                throw new InvalidOperationException(
                    $"Deserialization of type '{typeName}' from assembly '{assemblyName}' is not allowed. " +
                    $"Only known DurableTask and core system types are permitted.");
            }

            return this.defaultBinder.BindToType(assemblyName, typeName);
        }

        /// <inheritdoc />
        public void BindToName(Type serializedType, out string assemblyName, out string typeName)
        {
            this.defaultBinder.BindToName(serializedType, out assemblyName, out typeName);
        }

        bool IsAssemblyAllowed(string assemblyName)
        {
            if (string.IsNullOrWhiteSpace(assemblyName))
            {
                // No assembly specified — let the default binder resolve it.
                return true;
            }

            return this.assemblyAllowCache.GetOrAdd(assemblyName, name =>
            {
                // Strip version/culture/publicKeyToken if present (e.g., "mscorlib, Version=4.0.0.0, ...")
                int commaIndex = name.IndexOf(',');
                string shortName = commaIndex >= 0 ? name.Substring(0, commaIndex).Trim() : name.Trim();
                return AllowedAssemblyNames.Contains(shortName);
            });
        }
    }
}
