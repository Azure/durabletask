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

namespace DurableTask.ServiceBus.Tracking
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using DurableTask.Core.History;
    using DurableTask.Core.Serializing;
    using Newtonsoft.Json;

    /// <summary>
    /// Strict <see cref="Newtonsoft.Json.Serialization.ISerializationBinder"/> used when deserializing
    /// orchestration history events stored in Azure Table tracking. Only types that the framework
    /// itself emits when serializing a <see cref="HistoryEvent"/> are accepted; any other
    /// <c>$type</c> token is rejected with a <see cref="JsonSerializationException"/>.
    /// </summary>
    /// <remarks>
    /// This binder is intentionally restrictive: the history event JSON written by DTFx never
    /// contains polymorphic customer types (customer payloads such as <c>Input</c>/<c>Result</c>/
    /// <c>Reason</c>/<c>Details</c> are pre-serialized strings on the <see cref="HistoryEvent"/>
    /// subtree and are opaque to this serializer). Restricting the resolvable type set defends
    /// against unsafe-deserialization gadget chains if a malicious <c>$type</c> were ever written
    /// into the tracking table by an attacker with write access to the Storage account.
    /// </remarks>
    internal sealed class HistoryEventSerializationBinder : PackageUpgradeSerializationBinder
    {
        static readonly Assembly DurableTaskCoreAssembly = typeof(HistoryEvent).Assembly;

        /// <inheritdoc />
        public override Type BindToType(string assemblyName, string typeName)
        {
            Type resolved = base.BindToType(assemblyName, typeName);

            if (resolved == null || !IsAllowed(resolved))
            {
                throw new JsonSerializationException(
                    $"Type '{typeName}' from assembly '{assemblyName}' is not permitted by the orchestration history serialization binder.");
            }

            return resolved;
        }

        static bool IsAllowed(Type type)
        {
            // Allow any type defined in DurableTask.Core (HistoryEvent subclasses, OrchestrationInstance,
            // ParentInstance, FailureDetails, OrchestrationExecutionContext, etc.), plus
            // Dictionary<string, string> to round-trip the IDictionary<string, string> Tags members
            // declared on ExecutionStartedEvent / SubOrchestrationInstanceCreatedEvent /
            // TaskScheduledEvent (Newtonsoft.Json emits a $type for these because the static
            // declared type is an interface).
            return type.Assembly == DurableTaskCoreAssembly
                || type == typeof(Dictionary<string, string>);
        }
    }
}
