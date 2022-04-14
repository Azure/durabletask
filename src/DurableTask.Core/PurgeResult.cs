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
    /// Class to hold statistics about this execution of purge history
    /// </summary>
    public class PurgeResult
    {
        /// <summary>
        /// Constructor for purge history statistics
        /// </summary>
        /// <param name="instancesDeleted">Number of instances deleted</param>
        public PurgeResult(int instancesDeleted)
        {
            this.InstancesDeleted = instancesDeleted;
        }

        /// <summary>
        /// Number of instances deleted during this execution of purge history
        /// </summary>
        public int InstancesDeleted { get; }
    }
}