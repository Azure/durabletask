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
        protected readonly PartitionSender sender;

        public EventSender(PartitionSender sender)
        {
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

                    var eventData = new EventData(new ArraySegment<byte>(stream.GetBuffer(), startpos, (int)stream.Position - startpos));

                    if (!batch.TryAdd(eventData))
                    {
                        if (batch.Count == 0)
                        {
                            throw new Exception("message too large"); // TODO
                        }
                        else
                        {
                            // send the batch we have so far
                            await sender.SendAsync(batch);

                            // create new batch and reset stream
                            stream.Seek(0, SeekOrigin.Begin);
                            batch = sender.CreateBatch();
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
                System.Diagnostics.Trace.TraceError($"!!! failure in sender: {e}");
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
