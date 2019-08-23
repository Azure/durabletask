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
using System.IO;
using System.Linq;
using System.Text;

namespace DurableTask.EventSourced
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