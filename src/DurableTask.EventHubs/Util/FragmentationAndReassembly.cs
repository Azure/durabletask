using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DurableTask.EventHubs
{
    internal static class FragmentationAndReassembly
    {
        private const int FragmentSize = 50000;

        public interface IEventFragment
        {
            Guid CohortId { get; }

            byte[] Bytes { get; }

            bool IsLast { get; }

            long QueuePosition { get; }
        }

        public static List<IEventFragment> Fragment(ArraySegment<byte> segment, Event original)
        {
            var cohortId = Guid.NewGuid();
            var list = new List<IEventFragment>();
            int offset = segment.Offset;
            int length = segment.Count;
            while (length > 0)
            {
                int portion = Math.Min(length, FragmentSize);
                if (original is ClientEvent clientEvent)
                {
                    list.Add(new ClientEventFragment()
                    {
                        ClientId = clientEvent.ClientId,
                        RequestId = clientEvent.RequestId,
                        CohortId = cohortId,
                        Bytes = new ArraySegment<byte>(segment.Array, offset, portion).ToArray(),
                        IsLast = (portion == length),
                    });
                }
                else if (original is PartitionEvent partitionEvent)
                {
                    list.Add(new PartitionEventFragment()
                    {
                        PartitionId = partitionEvent.PartitionId,
                        CohortId = cohortId,
                        Bytes = new ArraySegment<byte>(segment.Array, offset, portion).ToArray(),
                        IsLast = (portion == length),
                    });
                }
                offset += portion;
                length -= portion;
            }
            return list;
        }

        public static Event Reassemble(IEnumerable<IEventFragment> earlierFragments, IEventFragment lastFragment)
        {
            using (var stream = new MemoryStream())
            {
                foreach (var x in earlierFragments)
                {
                    stream.Write(x.Bytes, 0, x.Bytes.Length);
                }
                stream.Write(lastFragment.Bytes, 0, lastFragment.Bytes.Length);
                var evt = Serializer.DeserializeEvent(new ArraySegment<byte>(stream.GetBuffer(), 0, (int)stream.Position));
                evt.QueuePosition = lastFragment.QueuePosition;
                return evt;
            }
        }
    }
}