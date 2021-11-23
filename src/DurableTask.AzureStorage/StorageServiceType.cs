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

namespace DurableTask.AzureStorage
{
    /// <summary>
    /// Specifies a data service within the Azure Storage platform.
    /// </summary>
    public enum StorageServiceType
    {
        /// <summary>
        /// Specifies blob storage, an object store, for text and binary data.
        /// </summary>
        Blob,

        /// <summary>
        /// Specifies queue storage for reliable messaging between application components.
        /// </summary>
        Queue,

        /// <summary>
        /// Specifies table storage, a NoSQL store, for schemaless storage of structured data.
        /// </summary>
        Table,
    }
}
