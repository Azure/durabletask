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
    using System;
    using System.Runtime.Serialization;
    using DurableTask.Core.History;

    /// <summary>
    /// Wire level transport object for task messages containing events and orchestration instance information
    /// </summary>
    [DataContract]
    public class TaskMessage : IExtensibleDataObject
    {
        /// <summary>
        /// Even information for this taks message
        /// </summary>
        [DataMember] public HistoryEvent Event;

        /// <summary>
        /// Sequence number for ordering of messages in history tracking
        /// </summary>
        [DataMember] public long SequenceNumber;

        /// <summary>
        /// The orchestration instance information
        /// </summary>
        [DataMember] public OrchestrationInstance OrchestrationInstance;

        /// <summary>
        /// Implementation for <see cref="IExtensibleDataObject.ExtensionData"/>.
        /// </summary>
        public ExtensionDataObject ExtensionData { get; set; }
    }
}