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
    using Newtonsoft.Json;

    /// <summary>
    /// Strict <see cref="Newtonsoft.Json.Serialization.ISerializationBinder"/> used by
    /// <see cref="TraceContextBase"/> when (de)serializing trace-context payloads that flow with
    /// orchestration messages. Only <see cref="TraceContextBase"/> and its subclasses are accepted;
    /// any other <c>$type</c> token is rejected with a <see cref="JsonSerializationException"/>.
    /// </summary>
    internal sealed class TraceContextSerializationBinder : PackageUpgradeSerializationBinder
    {
        static readonly Type TraceContextBaseType = typeof(TraceContextBase);
        static readonly Type StackOfTraceContextBaseType = typeof(Stack<TraceContextBase>);

        /// <inheritdoc />
        public override Type BindToType(string assemblyName, string typeName)
        {
            Type resolved = base.BindToType(assemblyName, typeName);

            if (!IsAllowed(resolved))
            {
                throw new JsonSerializationException(
                    $"Type '{resolved.FullName}, {resolved.Assembly.GetName().Name}' is not permitted by the trace-context type allowlist.");
            }

            return resolved;
        }

        static bool IsAllowed(Type type)
        {
            if (type == TraceContextBaseType || type.IsSubclassOf(TraceContextBaseType))
            {
                return true;
            }

            if (type == StackOfTraceContextBaseType)
            {
                return true;
            }

            return false;
        }
    }
}
