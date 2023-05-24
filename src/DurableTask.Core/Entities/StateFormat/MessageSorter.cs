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

namespace DurableTask.Core.Entities
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.Serialization;
    using DurableTask.Core.Entities.EventFormat;

    /// <summary>
    /// provides message ordering and deduplication of request messages (operations or lock requests)
    /// that are sent to entities, from other entities, or from orchestrations.
    /// </summary>
    [DataContract]
    internal class MessageSorter
    {
        // don't update the reorder window too often since the garbage collection incurs some overhead.
        private static readonly TimeSpan MinIntervalBetweenCollections = TimeSpan.FromSeconds(10);

        [DataMember(EmitDefaultValue = false)]
        public Dictionary<string, DateTime> LastSentToInstance { get; set; }

        [DataMember(EmitDefaultValue = false)]
        public Dictionary<string, ReceiveBuffer> ReceivedFromInstance { get; set; }

        [DataMember(EmitDefaultValue = false)]
        public DateTime ReceiveHorizon { get; set; }

        [DataMember(EmitDefaultValue = false)]
        public DateTime SendHorizon { get; set; }

        /// <summary>
        /// Used for testing purposes.
        /// </summary>
        [IgnoreDataMember]
        internal int NumberBufferedRequests =>
            ReceivedFromInstance?.Select(kvp => kvp.Value.Buffered?.Count ?? 0).Sum() ?? 0;

        /// <summary>
        /// Called on the sending side, to fill in timestamp and predecessor fields.
        /// </summary>
        public void LabelOutgoingMessage(RequestMessage message, string destination, DateTime now, TimeSpan reorderWindow)
        {
            if (reorderWindow.Ticks == 0)
            {
                return; // we are not doing any message sorting.
            }

            DateTime timestamp = now;

            // whenever (SendHorizon + reorderWindow < now) it is possible to advance the send horizon to (now - reorderWindow)
            // and we can then clean out all the no-longer-needed entries of LastSentToInstance.
            // However, to reduce the overhead of doing this collection, we don't update the send horizon immediately when possible.
            // Instead, we make sure at least MinIntervalBetweenCollections passes between collections.
            if (SendHorizon + reorderWindow + MinIntervalBetweenCollections < now)
            {
                SendHorizon = now - reorderWindow;

                // clean out send clocks that are past the reorder window

                if (LastSentToInstance != null)
                {
                    List<string> expired = new List<string>();

                    foreach (var kvp in LastSentToInstance)
                    {
                        if (kvp.Value < SendHorizon)
                        {
                            expired.Add(kvp.Key);
                        }
                    }

                    foreach (var t in expired)
                    {
                        LastSentToInstance.Remove(t);
                    }
                }
            }

            if (LastSentToInstance == null)
            {
                LastSentToInstance = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
            }
            else if (LastSentToInstance.TryGetValue(destination, out var last))
            {
                message.Predecessor = last;

                // ensure timestamps are monotonic even if system clock is not
                if (timestamp <= last)
                {
                    timestamp = new DateTime(last.Ticks + 1);
                }
            }

            message.Timestamp = timestamp;
            LastSentToInstance[destination] = timestamp;
        }

        /// <summary>
        /// Called on the receiving side, to reorder and deduplicate within the window.
        /// </summary>
        public IEnumerable<RequestMessage> ReceiveInOrder(RequestMessage message, TimeSpan reorderWindow)
        {
            // messages sent from clients and forwarded lock messages are not participating in the sorting.
            if (reorderWindow.Ticks == 0 || message.ParentInstanceId == null || message.Position > 0)
            {
                // Just pass the message through.
                yield return message;
                yield break;
            }

            // whenever (ReceiveHorizon + reorderWindow < message.Timestamp), we can advance the receive horizon to (message.Timestamp - reorderWindow)
            // and then we can clean out all the no-longer-needed entries of ReceivedFromInstance.
            // However, to reduce the overhead of doing this collection, we don't update the receive horizon immediately when possible.
            // Instead, we make sure at least MinIntervalBetweenCollections passes between collections.
            if (ReceiveHorizon + reorderWindow + MinIntervalBetweenCollections < message.Timestamp)
            {
                ReceiveHorizon = message.Timestamp - reorderWindow;

                // deliver any messages that were held in the receive buffers
                // but are now past the reorder window

                List<string> buffersToRemove = new List<string>();

                if (ReceivedFromInstance != null)
                {
                    foreach (var kvp in ReceivedFromInstance)
                    {
                        if (kvp.Value.Last < ReceiveHorizon)
                        {
                            // we reset Last to MinValue; this means all future messages received
                            // are treated as if they were the first message received.  
                            kvp.Value.Last = DateTime.MinValue;
                        }

                        while (TryDeliverNextMessage(kvp.Value, out var next))
                        {
                            yield return next;
                        }

                        if (kvp.Value.Last == DateTime.MinValue
                            && (kvp.Value.Buffered == null || kvp.Value.Buffered.Count == 0))
                        {
                            // we no longer need to store this buffer since it contains no relevant information anymore
                            // (it is back to its initial "empty" state)
                            buffersToRemove.Add(kvp.Key);
                        }
                    }

                    foreach (var t in buffersToRemove)
                    {
                        ReceivedFromInstance.Remove(t);
                    }

                    if (ReceivedFromInstance.Count == 0)
                    {
                        ReceivedFromInstance = null;
                    }
                }
            }

            // Messages older than the reorder window are not participating.
            if (message.Timestamp < ReceiveHorizon)
            {
                // Just pass the message through.
                yield return message;
                yield break;
            }

            ReceiveBuffer receiveBuffer;

            if (ReceivedFromInstance == null)
            {
                ReceivedFromInstance = new Dictionary<string, ReceiveBuffer>(StringComparer.OrdinalIgnoreCase);
            }

            if (!ReceivedFromInstance.TryGetValue(message.ParentInstanceId, out receiveBuffer))
            {
                ReceivedFromInstance[message.ParentInstanceId] = receiveBuffer = new ReceiveBuffer()
                {
                    ExecutionId = message.ParentExecutionId,
                };
            }
            else if (receiveBuffer.ExecutionId != message.ParentExecutionId)
            {
                // this message is from a new execution; release all buffered messages and start over
                if (receiveBuffer.Buffered != null)
                {
                    foreach (var kvp in receiveBuffer.Buffered)
                    {
                        yield return kvp.Value;
                    }

                    receiveBuffer.Buffered.Clear();
                }

                receiveBuffer.Last = DateTime.MinValue;
                receiveBuffer.ExecutionId = message.ParentExecutionId;
            }

            if (message.Timestamp <= receiveBuffer.Last)
            {
                // This message was already delivered, it's a duplicate
                yield break;
            }

            if (message.Predecessor > receiveBuffer.Last
                && message.Predecessor >= ReceiveHorizon)
            {
                // this message is waiting for a non-delivered predecessor in the window, buffer it
                if (receiveBuffer.Buffered == null)
                {
                    receiveBuffer.Buffered = new SortedDictionary<DateTime, RequestMessage>();
                }

                receiveBuffer.Buffered[message.Timestamp] = message;
            }
            else
            {
                yield return message;

                receiveBuffer.Last = message.Timestamp >= ReceiveHorizon ? message.Timestamp : DateTime.MinValue;

                while (TryDeliverNextMessage(receiveBuffer, out var next))
                {
                    yield return next;
                }
            }
        }

        private bool TryDeliverNextMessage(ReceiveBuffer buffer, out RequestMessage message)
        {
            if (buffer.Buffered != null)
            {
                using (var e = buffer.Buffered.GetEnumerator())
                {
                    if (e.MoveNext())
                    {
                        var pred = e.Current.Value.Predecessor;

                        if (pred <= buffer.Last || pred < ReceiveHorizon)
                        {
                            message = e.Current.Value;

                            buffer.Last = message.Timestamp >= ReceiveHorizon ? message.Timestamp : DateTime.MinValue;

                            buffer.Buffered.Remove(message.Timestamp);

                            return true;
                        }
                    }
                }
            }

            message = null;
            return false;
        }

        [DataContract]
        public class ReceiveBuffer
        {
            [DataMember]
            public DateTime Last { get; set; }// last message delivered, or DateTime.Min if none

            [DataMember(EmitDefaultValue = false)]
            public string ExecutionId { get; set; } // execution id of last message, if any

            [DataMember(EmitDefaultValue = false)]
            public SortedDictionary<DateTime, RequestMessage> Buffered { get; set; }
        }
    }
}
