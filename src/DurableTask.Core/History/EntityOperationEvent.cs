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

namespace DurableTask.Core.History
{
    /// <summary>
    /// A special case of <see cref="EventRaisedEvent"/> for entities-as-orchestrations.
    /// </summary>
    public class EntityMessageEvent : EventRaisedEvent
    {
        /// <summary>
        /// Creates a new <see cref="EntityMessageEvent"/> with the supplied event id and input.
        /// </summary>
        /// <param name="eventId">The ID of the event.</param>
        /// <param name="input">The serialized operation payload.</param>
        public EntityMessageEvent(int eventId, string input)
            : base(eventId, input)
        {
        }
    }
}
