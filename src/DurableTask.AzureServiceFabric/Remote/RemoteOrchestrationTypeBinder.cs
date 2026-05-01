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

namespace DurableTask.AzureServiceFabric.Remote
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using DurableTask.Core;
    using DurableTask.Core.Serializing;
    using Newtonsoft.Json;

    /// <summary>
    /// Strict <see cref="Newtonsoft.Json.Serialization.ISerializationBinder"/> used by
    /// <see cref="RemoteOrchestrationServiceClient"/> when configuring the JSON formatter that
    /// serializes orchestration RPC payloads (<c>TaskMessage</c>, <c>CreateTaskOrchestrationParameters</c>,
    /// etc.). Only types defined in <c>DurableTask.Core</c> and <c>DurableTask.AzureServiceFabric</c>,
    /// plus <see cref="Dictionary{TKey, TValue}"/> of strings, are accepted; any other <c>$type</c>
    /// token is rejected with a <see cref="JsonSerializationException"/>.
    /// </summary>
    /// <remarks>
    /// The formatter that uses this binder is only consumed for outbound serialization in
    /// <see cref="RemoteOrchestrationServiceClient"/>; the binder is provided as defense in depth so
    /// that the same settings remain safe if reused for deserialization.
    /// </remarks>
    internal sealed class RemoteOrchestrationTypeBinder : PackageUpgradeSerializationBinder
    {
        static readonly Assembly DurableTaskCoreAssembly = typeof(TaskMessage).Assembly;
        static readonly Assembly DurableTaskAzureServiceFabricAssembly = typeof(RemoteOrchestrationTypeBinder).Assembly;

        /// <inheritdoc />
        public override Type BindToType(string assemblyName, string typeName)
        {
            Type resolved = base.BindToType(assemblyName, typeName);

            if (resolved == null || !IsAllowed(resolved))
            {
                throw new JsonSerializationException(
                    $"Type '{typeName}' from assembly '{assemblyName}' is not permitted by the remote orchestration serialization binder.");
            }

            return resolved;
        }

        static bool IsAllowed(Type type)
        {
            // Allow types defined in DurableTask.Core (TaskMessage, HistoryEvent subclasses,
            // OrchestrationInstance, etc.) and DurableTask.AzureServiceFabric (CreateTaskOrchestrationParameters,
            // PurgeOrchestrationHistoryParameters, etc.), plus Dictionary<string, string> for the
            // IDictionary<string, string> Tags members on history events.
            return type.Assembly == DurableTaskCoreAssembly
                || type.Assembly == DurableTaskAzureServiceFabricAssembly
                || type == typeof(Dictionary<string, string>);
        }
    }
}
