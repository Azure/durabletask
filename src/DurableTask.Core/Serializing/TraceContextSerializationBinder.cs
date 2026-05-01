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

namespace DurableTask.Core.Serializing
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using Newtonsoft.Json;

    /// <summary>
    /// Strict <see cref="Newtonsoft.Json.Serialization.ISerializationBinder"/> used by
    /// <see cref="TraceContextBase"/> when (de)serializing trace-context payloads that flow with
    /// orchestration messages. Only <see cref="TraceContextBase"/> and its subclasses (plus
    /// <see cref="Stack{TraceContextBase}"/> for the <c>OrchestrationTraceContexts</c> member)
    /// are accepted; any other <c>$type</c> token is rejected with a
    /// <see cref="JsonSerializationException"/>.
    /// </summary>
    /// <remarks>
    /// Restricting the resolvable type set defends against unsafe-deserialization gadget chains
    /// if a malicious <c>$type</c> were ever injected into a trace-context payload by an attacker
    /// with the ability to write into the orchestration message stream.
    /// </remarks>
    internal sealed class TraceContextSerializationBinder : PackageUpgradeSerializationBinder
    {
        static readonly Type TraceContextBaseType = typeof(TraceContextBase);
        static readonly Type StackOfTraceContextBaseType = typeof(Stack<TraceContextBase>);
        static readonly Assembly DurableTaskCoreAssembly = typeof(TraceContextBase).Assembly;

        // Allowlist of simple assembly names whose types may even be resolved by this binder.
        // This is a defense-in-depth pre-filter applied *before* delegating to the base binder so
        // that the .NET runtime never has to load (and execute the module initializer of) an
        // assembly that is not on the allowlist as a side effect of probing a malicious $type.
        // The set spans:
        //   * DurableTask.Core (the source of TraceContextBase and friends).
        //   * The BCL host of Stack<TraceContextBase> for the OrchestrationTraceContexts member.
        //   * The legacy pre-v2 DTFx assembly names rewritten by PackageUpgradeSerializationBinder.
        static readonly HashSet<string> AllowedAssemblySimpleNames = new HashSet<string>(StringComparer.Ordinal)
        {
            DurableTaskCoreAssembly.GetName().Name,                       // DurableTask.Core
            StackOfTraceContextBaseType.Assembly.GetName().Name,          // System.Collections / mscorlib / System.Private.CoreLib
            "DurableTask",                                                // pre-v2 DTFx assembly (legacy upgrade path)
            "DurableTaskFx",                                              // pre-v2 DTFx vNext assembly (legacy upgrade path)
        };

        /// <inheritdoc />
        public override Type BindToType(string assemblyName, string typeName)
        {
            // Stage 1: reject by assembly name string before invoking the base binder so that
            // unknown assemblies are never loaded just to be rejected afterwards. Also rejects
            // null/empty assembly names deterministically: Json.NET can invoke BindToType with a
            // null assemblyName when an incoming $type token omits the assembly portion, and the
            // base PackageUpgradeSerializationBinder would NRE in that case.
            string simpleAssemblyName = ExtractSimpleAssemblyName(assemblyName);
            if (simpleAssemblyName == null || !AllowedAssemblySimpleNames.Contains(simpleAssemblyName))
            {
                throw new JsonSerializationException(
                    $"Type '{typeName}' from assembly '{assemblyName}' is not permitted by the trace-context serialization binder.");
            }

            // Stage 2: delegate to PackageUpgradeSerializationBinder for the legacy
            // DurableTask.* -> DurableTask.Core.* rewrite and standard type resolution.
            Type resolved = base.BindToType(assemblyName, typeName);

            // Stage 3: filter the resolved type. Even though Stage 1 narrowed the assembly set,
            // the BCL is in scope (so that Stack<TraceContextBase> can round-trip), so we still
            // need this post-resolution check to reject other BCL types that an attacker might
            // try to use as a deserialization gadget.
            if (resolved == null || !IsAllowed(resolved))
            {
                throw new JsonSerializationException(
                    $"Type '{typeName}' from assembly '{assemblyName}' is not permitted by the trace-context serialization binder.");
            }

            return resolved;
        }

        static bool IsAllowed(Type type)
        {
            // TraceContextBase itself or any subclass (W3CTraceContext, HttpCorrelationProtocolTraceContext,
            // NullObjectTraceContext, plus any future subclass added in DurableTask.Core).
            if (TraceContextBaseType.IsAssignableFrom(type))
            {
                return true;
            }

            // Stack<TraceContextBase> may be requested when the static type of the
            // OrchestrationTraceContexts property is bound. The generic argument's identity is
            // already constrained by the type system, so allowing this exact closed generic
            // type does not widen the gadget surface beyond TraceContextBase itself.
            if (type == StackOfTraceContextBaseType)
            {
                return true;
            }

            return false;
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
