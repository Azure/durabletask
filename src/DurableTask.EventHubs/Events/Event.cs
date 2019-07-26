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

using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;

namespace DurableTask.EventHubs
{
    [DataContract]
    [KnownType(typeof(CreationResponseReceived))]
    [KnownType(typeof(StateResponseReceived))]
    [KnownType(typeof(WaitResponseReceived))]
    [KnownType(typeof(ClientTaskMessagesReceived))]
    [KnownType(typeof(CreationRequestReceived))]
    [KnownType(typeof(StateRequestReceived))]
    [KnownType(typeof(WaitRequestReceived))]
    [KnownType(typeof(ActivityCompleted))]
    [KnownType(typeof(BatchProcessed))]
    [KnownType(typeof(SentMessagesAcked))]
    [KnownType(typeof(TimerFired))]
    [KnownType(typeof(TaskMessageReceived))]
    internal abstract class Event
    {
        /// <summary>
        /// For received events, this is the queue position at which the event was received, and is filled in by the back-end.
        /// For sent events, this is the event that caused the send (for non-impulses), or -1 (for impulses).
        /// </summary>
        [IgnoreDataMember]
        public long QueuePosition { get; set; } = -1;

        public override string ToString()
        {
            var s = new StringBuilder();
            s.Append(this.GetType().Name);
            this.AddExtraInformation(s);
            return s.ToString();
        }

        protected virtual void AddExtraInformation(StringBuilder s)
        {
        }
    }
}