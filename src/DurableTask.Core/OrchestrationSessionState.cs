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
    using System.Collections.Generic;
    using DurableTask.Core.History;

    /// <summary>
    /// The object that represents the serialized session state.
    /// It holds a list of history events (when blob key is empty),
    /// or a key for external storage if the serialized stream is too large to fit into the the session state.
    /// </summary>
    internal class OrchestrationSessionState
    {
        /// <summary>
        /// A constructor for deserialzation.
        /// </summary>
        public OrchestrationSessionState()
        {
        }

        /// <summary>
        /// Wrap a list of history events into an OrchestrationSessionState instance, which will be later serialized as a stream saved in session state.
        /// </summary>
        /// /// <param name="events">A list of history events.</param>
        public OrchestrationSessionState(IList<HistoryEvent> events)
        {
            this.Events = events;
        }

        /// <summary>
        /// Construct an OrchestrationSessionState instance with a blob key as the blob reference in the external blob storage.
        /// </summary>
        /// /// <param name="blobKey">The blob key to access the blob</param>
        public OrchestrationSessionState(string blobKey)
        {
            this.BlobKey = blobKey;
        }

        /// <summary>
        /// List of all history events for runtime state
        /// </summary>
        public IList<HistoryEvent> Events { get; set; }

        /// <summary>
        /// The blob key for external storage. Could be null or empty if not externally stored.
        /// </summary>
        public string BlobKey { get; set; }
    }
}
