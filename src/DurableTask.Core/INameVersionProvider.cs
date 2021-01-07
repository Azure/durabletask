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
    /// An interface for supplying names and versions for a given object.
    /// </summary>
    public interface INameVersionProvider
    {
        /// <summary>
        /// Gets the name of an Object instance.
        /// </summary>
        /// <param name="obj">Object to get the name for</param>
        /// <param name="useFullyQualifiedMethodNames">Boolean indicating whether to use fully qualified names or not</param>
        /// <returns>Name of the object instance's type</returns>
        string GetName(object obj, bool useFullyQualifiedMethodNames = false);

        /// <summary>
        /// Gets the default version for an object.
        /// </summary>
        /// <param name="obj">Object to get the version for</param>
        /// <returns>The version as string</returns>
        string GetVersion(object obj);
    }
}
