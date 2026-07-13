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
    /// Represents the state of a live migration away from the Azure Storage backend.
    /// </summary>
    enum MigrationState
    {
        /// <summary>
        /// No migration has been started. This is the default state.
        /// </summary>
        NotStarted = 0,

        /// <summary>
        /// A migration has started. While in this state, the backend records modified instances in the
        /// modified-instances queue and increments the per-instance sequence number on every write.
        /// </summary>
        Started = 1,

        /// <summary>
        /// A migration has completed. A backend that reads this state on startup must immediately shut down.
        /// </summary>
        Completed = 2,
    }
}
