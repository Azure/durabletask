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
#nullable enable
namespace DurableTask.Core
{
    /// <summary>
    /// A class representing metadata information about a work item.
    /// </summary>
    public class WorkItemMetadata
    {
        internal WorkItemMetadata(bool isExtendedSession, bool includeState)
            : this(isExtendedSession, includeState, deliveryAttempt: null)
        {
        }

        internal WorkItemMetadata(bool isExtendedSession, bool includeState, long? deliveryAttempt)
        {
            this.IsExtendedSession = isExtendedSession;
            this.IncludeState = includeState;
            this.DeliveryAttempt = deliveryAttempt;
        }

        /// <summary>
        /// Gets whether or not the execution of the work item is within an extended session. 
        /// </summary>
        public bool IsExtendedSession { get; private set; }

        /// <summary>
        /// Gets whether or not to include instance state when executing the work item via middleware.
        /// When false, this assumes that the middleware is able to handle extended sessions and has already cached
        /// the instance state from a previous execution, so it does not need to be included again.
        /// </summary>
        public bool IncludeState { get; private set; }

        /// <summary>
        /// Gets or sets the one-based delivery attempt number for the current work item.
        /// </summary>
        /// <remarks>
        /// This value does not represent the attempt number of a Durable Task retry policy, and
        /// does not necessarily reflect the amount of times user code has been executed.
        /// A value of <c>null</c> indicates that the delivery attempt number is not available.
        /// </remarks>
        public long? DeliveryAttempt { get; private set; }
    }
}
