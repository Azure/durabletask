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
    /// The live-migration mode to run the Azure Storage backend in, supplied to
    /// <see cref="AzureStorageOrchestrationService.StartAsync(MigrationMode)"/>.
    /// </summary>
    public enum MigrationMode
    {
        /// <summary>
        /// A migration is in progress. The backend records modified instances in the modified-instances queue and
        /// increments the per-instance sequence number on every write.
        /// </summary>
        MigrationStarted,

        /// <summary>
        /// A migration has ended. On startup the backend records a durable marker in storage and then runs normally.
        /// </summary>
        MigrationEnding,
    }
}
