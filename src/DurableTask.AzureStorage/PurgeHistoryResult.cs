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
    /// Class to hold statistics about this execution of purge history
    /// </summary>
    public class PurgeHistoryResult
    {
        /// <summary>
        /// Constructor for purge history statistics
        /// </summary>
        /// <param name="storageRequests">Requests sent to storage</param>
        /// <param name="instancesDeleted">Number of instances deleted</param>
        /// <param name="rowsDeleted">Number of rows deleted</param>
        public PurgeHistoryResult(int storageRequests, int instancesDeleted, int rowsDeleted)
        {
            this.StorageRequests = storageRequests;
            this.InstancesDeleted = instancesDeleted;
            this.RowsDeleted = rowsDeleted;
        }

        /// <summary>
        /// Number of requests sent to Storage during this execution of purge history
        /// </summary>
        public int StorageRequests { get; }

        /// <summary>
        /// Number of instances deleted during this execution of purge history
        /// </summary>
        public int InstancesDeleted { get; }

        /// <summary>
        /// Number of rows deleted during this execution of purge history
        /// </summary>
        public int RowsDeleted { get; }
    }
}
