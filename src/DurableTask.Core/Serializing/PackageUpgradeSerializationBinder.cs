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
    using System.Linq;
    using Newtonsoft.Json.Serialization;

    /// <summary>
    /// SerializationBinder to be used for deserializing DurableTask types that are pre v-2.0, this allows upgrade compaibility. This is not sufficient to deserialize objects from 1.0 which had the Tags Property set.
    /// </summary>
    public class PackageUpgradeSerializationBinder : DefaultSerializationBinder
    {
        static Lazy<IDictionary<string, Type>> KnownTypes = new Lazy<IDictionary<string, Type>>(() =>
        {
            return typeof(PackageUpgradeSerializationBinder).Assembly.GetTypes().Where(t => t?.Namespace?.StartsWith("DurableTask.Core") ?? false).ToDictionary<Type, string>(x=>x.FullName);
        });

        //Keeping both to avoid Using ctring concatenation to create a new string with a , everytime that the GC needs to clean
        private static readonly string[] _upgradeableAssemblyNames = new string[] { "DurableTask", "DurableTaskFx" };
        private static readonly string[] _upgradeableAssemblyNamePrefixes = new string[] { "DurableTask,", "DurableTaskFx," };
        /// <inheritdoc />
        public override Type BindToType(string assemblyName, string typeName)
        {
            Type resolvedType = null;

            if (!string.IsNullOrWhiteSpace(typeName))
            {
                //If no assembly name is specified or this is a type from the v1.0 or vnext assemblies
                if (string.IsNullOrWhiteSpace(assemblyName) || _upgradeableAssemblyNames.Any(x=> x== assemblyName) || _upgradeableAssemblyNamePrefixes.Any(x => x.StartsWith(assemblyName)))
                {
                    KnownTypes.Value.TryGetValue(typeName.Replace("DurableTask.", "DurableTask.Core."), out resolvedType);
                }
            }

            if (resolvedType == null)
            {
                resolvedType = base.BindToType(assemblyName, typeName);
            }

            return resolvedType;
        }
    };
}
