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

namespace DurableTask.Emulator
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using DurableTask.Core.History;
    using DurableTask.Core.Serializing;
    using Newtonsoft.Json;

    /// <summary>
    /// Strict <see cref="Newtonsoft.Json.Serialization.ISerializationBinder"/> used by the in-memory
    /// emulator to round-trip the <see cref="IList{T}"/> of <see cref="HistoryEvent"/> that
    /// represents an orchestration's runtime state. Only types defined in <c>DurableTask.Core</c>,
    /// plus <see cref="Dictionary{TKey, TValue}"/> of strings, are accepted; any other <c>$type</c>
    /// token is rejected with a <see cref="JsonSerializationException"/>.
    /// </summary>
    public sealed class HistoryEventSerializationBinder : PackageUpgradeSerializationBinder
    {
        static readonly Assembly DurableTaskCoreAssembly = typeof(HistoryEvent).Assembly;

        /// <inheritdoc />
        public override Type BindToType(string assemblyName, string typeName)
        {
            Type resolved = base.BindToType(assemblyName, typeName);

            if (resolved == null || !IsAllowed(resolved))
            {
                throw new JsonSerializationException(
                    $"Type '{typeName}' from assembly '{assemblyName}' is not permitted by the orchestration runtime state serialization binder.");
            }

            return resolved;
        }

        static bool IsAllowed(Type type)
        {
            // Allow types defined in DurableTask.Core (HistoryEvent subclasses, OrchestrationInstance,
            // ParentInstance, FailureDetails, etc.), plus Dictionary<string, string> for the
            // IDictionary<string, string> Tags members declared on ExecutionStartedEvent /
            // SubOrchestrationInstanceCreatedEvent / TaskScheduledEvent (Newtonsoft.Json emits a
            // $type for these because the static declared type is an interface).
            return type.Assembly == DurableTaskCoreAssembly
                || type == typeof(Dictionary<string, string>);
        }
    }
}
