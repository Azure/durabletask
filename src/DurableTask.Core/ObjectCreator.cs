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
    /// <summary>
    /// Abstract class for object creation based on name and version
    /// </summary>
    /// <typeparam name="T">The type to create</typeparam>
    public abstract class ObjectCreator<T> : INameVersionInfo
    {
        /// <summary>
        /// The name of the method
        /// </summary>
        public string Name { get; protected set; }

        /// <summary>
        /// The version of the method
        /// </summary>
        public string Version { get; protected set; }

        /// <summary>
        /// Instance creator method
        /// </summary>
        /// <returns>An intance of the type T</returns>
        public abstract T Create();
    }
}