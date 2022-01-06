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
namespace DurableTask.Core.History
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;

    /// <summary>
    /// Base class for history events
    /// </summary>
    [DataContract]
    [KnownType(nameof(KnownTypes))]
    public abstract class HistoryEvent : IExtensibleDataObject
    {
        private static IReadOnlyCollection<Type>? knownTypes;

        /// <summary>
        /// List of all event classes, for use by the DataContractSerializer
        /// </summary>
        /// <returns>An enumerable of all known types that implement <see cref="HistoryEvent"/>.</returns>
        public static IEnumerable<Type> KnownTypes()
        {
            if (knownTypes != null)
            {
                return knownTypes;
            }

            knownTypes = typeof(HistoryEvent).Assembly
                .GetTypes()
                .Where(x => !x.IsAbstract && typeof(HistoryEvent).IsAssignableFrom(x))
                .ToList()
                .AsReadOnly();

            return knownTypes;
        }

        /// <summary>
        /// Creates a new history event
        /// </summary>
        internal HistoryEvent()
        {
            Timestamp = DateTime.UtcNow;
        }

        /// <summary>
        /// Creates a new history event with the supplied event id
        /// </summary>
        /// <param name="eventId">The integer event id</param>
        protected HistoryEvent(int eventId)
        {
            EventId = eventId;
            Timestamp = DateTime.UtcNow;
        }

        /// <summary>
        /// Gets the event id
        /// </summary>
        [DataMember]
        public int EventId { get; internal set; }

        /// <summary>
        /// Gets the IsPlayed status
        /// </summary>
        [DataMember]
        public bool IsPlayed { get; set; }

        /// <summary>
        /// Gets the event timestamp
        /// </summary>
        [DataMember]
        public DateTime Timestamp { get; set; }

        /// <summary>
        /// Gets the event type
        /// </summary>
        [DataMember]
        public virtual EventType EventType { get; private set; }

        /// <summary>
        /// Implementation for <see cref="IExtensibleDataObject.ExtensionData"/>.
        /// </summary>
        public ExtensionDataObject? ExtensionData { get; set; }
    }
}