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

        // Allowlist of simple assembly names whose types may even be resolved by this binder.
        // This is a defense-in-depth check applied *before* delegating to the base binder, so
        // that the .NET runtime never has to load (and execute the module initializer of) an
        // assembly that is not on the allowlist as a side effect of probing a malicious $type.
        // The set spans:
        //   * DurableTask.Core (the source of HistoryEvent and friends).
        //   * The legacy pre-v2 DTFx assembly names rewritten by PackageUpgradeSerializationBinder.
        //   * The BCL assemblies that host common IDictionary<string, string> implementations
        //     emitted as $type for the Tags member. The host assembly varies by TFM and by
        //     concrete dictionary type, so we enumerate them via typeof(...).Assembly to stay
        //     correct under multi-targeting.
        static readonly HashSet<string> AllowedAssemblySimpleNames = new HashSet<string>(StringComparer.Ordinal)
        {
            DurableTaskCoreAssembly.GetName().Name,                                                  // DurableTask.Core
            typeof(Dictionary<string, string>).Assembly.GetName().Name,                              // mscorlib / System.Private.CoreLib
            typeof(SortedDictionary<string, string>).Assembly.GetName().Name,                        // System / System.Collections
            typeof(System.Collections.Concurrent.ConcurrentDictionary<string, string>).Assembly.GetName().Name, // mscorlib / System.Collections.Concurrent
            typeof(System.Collections.ObjectModel.ReadOnlyDictionary<string, string>).Assembly.GetName().Name,  // mscorlib / System.ObjectModel
            "DurableTask",                                                                            // pre-v2 DTFx assembly (legacy upgrade path)
            "DurableTaskFx",                                                                          // pre-v2 DTFx vNext assembly (legacy upgrade path)
        };

        /// <inheritdoc />
        public override Type BindToType(string assemblyName, string typeName)
        {
            // Stage 1: reject by assembly name string before invoking the base binder so that
            // unknown assemblies are never loaded just to be rejected afterwards.
            // Reject null/empty assembly names deterministically. Json.NET can invoke
            // BindToType with a null assemblyName when an incoming $type token omits the
            // assembly portion; an unqualified type name is never produced by DTFx
            // serialization and would also cause the base PackageUpgradeSerializationBinder
            // to throw a NullReferenceException, so fail fast with a typed exception here.
            string simpleAssemblyName = ExtractSimpleAssemblyName(assemblyName);
            if (simpleAssemblyName == null || !AllowedAssemblySimpleNames.Contains(simpleAssemblyName))
            {
                throw new JsonSerializationException(
                    $"Type '{typeName}' from assembly '{assemblyName}' is not permitted by the orchestration history serialization binder.");
            }

            // Stage 2: delegate to PackageUpgradeSerializationBinder for the legacy
            // DurableTask.* -> DurableTask.Core.* rewrite and standard type resolution.
            Type resolved = base.BindToType(assemblyName, typeName);

            // Stage 3: filter the resolved type. The BCL is in the assembly allowlist (so that
            // Dictionary<string,string> can be round-tripped for the Tags member), so we still
            // need this post-resolution check to reject other BCL types (e.g., FileInfo,
            // ObjectDataProvider) that an attacker might try to use as a deserialization gadget.
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
            // ParentInstance, FailureDetails, OrchestrationExecutionContext, etc.).
            if (type.Assembly == DurableTaskCoreAssembly)
            {
                return true;
            }

            // For all other allowlisted assemblies (the BCL dictionary hosts plus the legacy
            // pre-v2 DTFx assemblies), only types assignable to IDictionary<string, string>
            // are accepted. This narrows the resolved set so that gadget types living in the
            // same allowlisted BCL assembly (e.g., FileInfo in System.Private.CoreLib) cannot
            // pass the post-resolution check. Tags is the only IDictionary<string, string>
            // member declared on a serialized HistoryEvent subtree, so any other concrete
            // type is unreachable by the legitimate serializer in any case.
            return typeof(IDictionary<string, string>).IsAssignableFrom(type);
        }

        static string ExtractSimpleAssemblyName(string assemblyName)
        {
            if (string.IsNullOrWhiteSpace(assemblyName))
            {
                return null;
            }
            int comma = assemblyName.IndexOf(',');
            return comma < 0 ? assemblyName : assemblyName.Substring(0, comma);
        }
    }
}
