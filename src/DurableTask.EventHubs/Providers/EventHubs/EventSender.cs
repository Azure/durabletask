using Microsoft.Azure.EventHubs;
//using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace DurableTask.EventHubs
{
    internal class EventSender<T> : BatchWorker where T: Event
    {
        private readonly PartitionSender sender;
        private readonly Backend.IHost host;

        public EventSender(Backend.IHost host, PartitionSender sender)
        {
            this.host = host;
            this.sender = sender;
        }

        private Object lockable = new object();
        private List<Entry> queue = new List<Entry>();
        private List<Entry> toSend = new List<Entry>();

        private struct Entry
        {
            public T Event;
            public Backend.ISendConfirmationListener Listener;
        }

        public void Add(T evt, Backend.ISendConfirmationListener listener)
        {
            lock (lockable)
            {
                queue.Add(new Entry() { Event = evt, Listener = listener });
            }
            Notify();
        }

        private readonly MemoryStream stream = new MemoryStream();

        protected override async Task Work()
        {
            try
            {
                lock (lockable)
                {
                    if (queue.Count == 0)
                        return;

                    // swap queues
                    var temp = this.toSend;
                    this.toSend = this.queue;
                    this.queue = temp;
                }

                // create and send as many batches as needed 
                var batch = sender.CreateBatch();
                
                for (int i = 0; i < toSend.Count; i++)
                {
                    var startpos = (int)stream.Position;

                    Serializer.SerializeEvent(toSend[i].Event, stream);

                    var arraySegment = new ArraySegment<byte>(stream.GetBuffer(), startpos, (int)stream.Position - startpos);
                    var eventData = new EventData(arraySegment);

                    if (batch.TryAdd(eventData))
                    {
                        continue;
                    }
                    else if (batch.Count > 0)
                    {
                        // send the batch we have so far, reset stream, and backtrack one element
                        await sender.SendAsync(batch);
                        batch = sender.CreateBatch();
                        stream.Seek(0, SeekOrigin.Begin);
                        i--;
                    }
                    else
                    {
                        // the message is too big. Break it into fragments, and send each individually.
                        var fragments = FragmentationAndReassembly.Fragment(arraySegment, toSend[i].Event);
                        foreach (var fragment in fragments)
                        {
                            stream.Seek(0, SeekOrigin.Begin);
                            Serializer.SerializeEvent((Event) fragment, stream);
                            await sender.SendAsync(new EventData(new ArraySegment<byte>(stream.GetBuffer(), 0, (int)stream.Position)));
                        }
                    }
                }

                if (batch.Count > 0)
                {
                    await sender.SendAsync(batch);
                }

                // finally, confirm the sends
                foreach (var t in toSend)
                {
                    t.Listener?.ConfirmDurablySent(t.Event);
                }

                // and clear the queue so we can reuse it for the next batch
                this.toSend.Clear();
            }
            catch (Exception e)
            {
                host.ReportError("Failure in sender", e);
            }
        }

        private IEnumerable<EventData> Serialize(IEnumerable<Entry> entries)
        {
            using (var stream = new MemoryStream())
            {
                foreach (var entry in entries)
                {
                    stream.Seek(0, SeekOrigin.Begin);
                    Serializer.SerializeEvent(entry.Event, stream);
                    var length = (int)stream.Position;
                    yield return new EventData(new ArraySegment<byte>(stream.GetBuffer(), 0, length));
                }
            }
        }
    }
}
