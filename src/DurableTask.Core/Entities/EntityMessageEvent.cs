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
using System;
using DurableTask.Core.Entities.EventFormat;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DurableTask.Core.Entities
{
    /// <summary>
    /// Encapsulates events that represent a message sent to or from an entity.
    /// </summary>
    public readonly struct EntityMessageEvent
    {
        readonly string eventName;
        readonly EntityMessage message;
        readonly EntityInstance target;

        internal EntityMessageEvent(string eventName, EntityMessage message, OrchestrationInstance target)
        {
            this.eventName = eventName;
            this.message = message;
            this.target = target as EntityInstance ?? EntityInstance.FromBase(target);
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            return this.message.ToString();
        }

        /// <summary>
        /// The name of the event.
        /// </summary>
        public string EventName => this.eventName;

        /// <summary>
        /// The target instance for the event.
        /// </summary>
        public OrchestrationInstance TargetInstance => this.target;

        /// <summary>
        /// Returns the content of this event, as an object that can be serialized later.
        /// </summary>
        /// <returns></returns>
        public object ContentAsObject()
        {
            // we pre-serialize this now to avoid interference from the application-defined serialization settings
            return JObject.FromObject(message, Serializer.InternalSerializer);
        }

        /// <summary>
        /// Returns the content of this event, as a serialized string.
        /// </summary>
        /// <returns></returns>
        public string ContentAsString()
        {
            return JsonConvert.SerializeObject(message, Serializer.InternalSerializerSettings);
        }

        /// <summary>
        /// Returns this event in the form of a TaskMessage.
        /// </summary>
        /// <returns></returns>
        public TaskMessage AsTaskMessage()
        {
            return new TaskMessage
            {
                OrchestrationInstance = this.target,
                Event = new History.EntityMessageEvent(-1, this.ContentAsString())
                {
                    Name = this.eventName
                }
            };
        }

        /// <summary>
        /// Utility function to compute a capped scheduled time, given a scheduled time, a timestamp representing the current time, and the maximum delay.
        /// </summary>
        /// <param name="nowUtc">a timestamp representing the current time</param>
        /// <param name="scheduledUtcTime">the scheduled time, or null if none.</param>
        /// <param name="maxDelay">The maximum delay supported by the backend.</param>
        /// <returns>the capped scheduled time, or null if none.</returns>
        public static (DateTime original, DateTime capped)? GetCappedScheduledTime(DateTime nowUtc, TimeSpan maxDelay, DateTime? scheduledUtcTime)
        {
            if (!scheduledUtcTime.HasValue)
            {
                return null;
            }

            if ((scheduledUtcTime - nowUtc) <= maxDelay)
            {
                return (scheduledUtcTime.Value, scheduledUtcTime.Value);
            }
            else
            {
                return (scheduledUtcTime.Value, nowUtc + maxDelay);
            }
        }
    }
}