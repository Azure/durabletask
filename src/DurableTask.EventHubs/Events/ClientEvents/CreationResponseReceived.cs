using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using DurableTask.Core;

namespace DurableTask.EventHubs
{
    [DataContract]
    internal class CreationResponseReceived : ClientEvent
    {
        [DataMember]
        public bool Succeeded { get; set; }
    }
}
