
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace DurableTask.EventHubs
{
    internal static class Serializer
    {
        private static DataContractSerializer eventSerializer 
            = new DataContractSerializer(typeof(Event));
        
        public static byte[] SerializeEvent(Event e)
        {
            var stream = new MemoryStream();
            eventSerializer.WriteObject(stream, e);
            return stream.ToArray();
        }

        public static void SerializeEvent(Event e, Stream s)
        {
            eventSerializer.WriteObject(s, e);
        }

        public static Event DeserializeEvent(ArraySegment<byte> bytes)
        {
            var stream = new MemoryStream(bytes.Array, bytes.Offset, bytes.Count);
            return (Event) eventSerializer.ReadObject(stream);
        }
    }
}
