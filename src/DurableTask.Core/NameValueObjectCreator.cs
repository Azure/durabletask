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

namespace DurableTask.Core
{
    using System;

    /// <summary>
    /// Object instance creator for a type using name and version mapping
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class NameValueObjectCreator<T> : DefaultObjectCreator<T>
    {
        /// <summary>
        /// Creates a new DefaultObjectCreator of supplied type with supplied name and version for mapping
        /// </summary>
        /// <param name="name">Lookup name for the type</param>
        /// <param name="version">Lookup version for the type</param>
        /// <param name="type">Type to use for the creator</param>
        public NameValueObjectCreator(string name, string version, Type type)
            : base(type)
        {
            Name = name;
            Version = version;
        }

        /// <summary>
        /// Creates a new DefaultObjectCreator of supplied object instance's type with supplied name and version for mapping
        /// </summary>
        /// <param name="name">Lookup name for the type</param>
        /// <param name="version">Lookup version for the type</param>
        /// <param name="instance">Object instances to infer the type from</param>
        public NameValueObjectCreator(string name, string version, T instance)
            : base(instance)
        {
            Name = name;
            Version = version;
        }
    }
}