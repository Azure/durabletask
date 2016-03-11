﻿//  ----------------------------------------------------------------------------------
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

namespace DurableTask.History
{
    using System;
    using System.Runtime.Serialization;

    [DataContract]
    public class TimerFiredEvent : HistoryEvent
    {
        public TimerFiredEvent(int eventId)
            : base(eventId)
        {
        }

        public override EventType EventType
        {
            get { return EventType.TimerFired; }
        }

        [DataMember]
        public int TimerId { get; set; }

        // AFFANDAR : TODO : wire format change. 
        [DataMember]
        public DateTime FireAt { get; set; }
    }
}