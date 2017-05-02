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
    /// Interface for name and version manager class to be used in type lookup mappings
    /// </summary>
    public interface INameVersionObjectManager<T>
    {
        /// <summary>
        /// Adds a new ObjectCreator to the name version Object manager
        /// </summary>
        /// <param name="creator">Class for creation of a new name and version instance</param>
        void Add(ObjectCreator<T> creator);

        /// <summary>
        /// Gets a creator class instance based on a name and version
        /// </summary>
        /// <param name="name">Name of the class to return the creator for</param>
        /// <param name="version">Version of the class to return the creator for</param>
        /// <returns>Class instance based on the matching creator class for the supplied name and version</returns>
        T GetObject(string name, string version);
    }
}