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
    /// Class representing the result of a purge operation.
    /// </summary>
    public class PurgeResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PurgeResult" /> class.
        /// </summary>
        /// <param name="deletedInstanceCount">The number of instances deleted.</param>
        public PurgeResult(int deletedInstanceCount)
        {
            this.DeletedInstanceCount = deletedInstanceCount;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PurgeResult" /> class.
        /// </summary>
        /// <param name="deletedInstanceCount">The number of instances deleted.</param>
        /// <param name="isComplete">A value indicating whether the purge operation is complete. 
        /// If true, the purge operation is complete. All instances were purged.
        /// If false, not all instances were purged. Please purge again.
        /// If null, whether or not all instances were purged is undefined.</param>
        public PurgeResult(int deletedInstanceCount, bool? isComplete)
        {
            this.DeletedInstanceCount = deletedInstanceCount;
            this.IsComplete = isComplete;
        }

        /// <summary>
        /// Number of instances deleted during this execution of the purge operation.
        /// </summary>
        public int DeletedInstanceCount { get; }

        /// <summary>
        /// Gets a value indicating whether the purge operation is complete.
        /// </summary>
        public bool? IsComplete { get; }
    }
}