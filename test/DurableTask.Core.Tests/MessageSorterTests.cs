// ---------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ---------------------------------------------------------------

namespace DurableTask.Core.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using DurableTask.Core.Entities;
    using DurableTask.Core.Entities.EventFormat;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class MessageSorterTests
    {
        private static readonly TimeSpan ReorderWindow = TimeSpan.FromMinutes(30);

        [TestMethod]
        public void SimpleInOrder()
        {
            var senderId = "A";
            var receiverId = "B";

            var senderSorter = new MessageSorter();

            var message1 = Send(senderId, receiverId, "1", senderSorter, DateTime.UtcNow);
            var message2 = Send(senderId, receiverId, "2", senderSorter, DateTime.UtcNow);
            var message3 = Send(senderId, receiverId, "3", senderSorter, DateTime.UtcNow);

            List<RequestMessage> batch;
            MessageSorter receiverSorter = new MessageSorter();

            // delivering the sequence in order produces 1 message each time
            batch = receiverSorter.ReceiveInOrder(message1, ReorderWindow).ToList();
            Assert.That.Single(batch).Input.Equals("1");
            batch = receiverSorter.ReceiveInOrder(message2, ReorderWindow).ToList();
            Assert.That.Single(batch).Input.Equals("2");
            batch = receiverSorter.ReceiveInOrder(message3, ReorderWindow).ToList();
            Assert.That.Single(batch).Input.Equals("3");

            Assert.AreEqual(0, receiverSorter.NumberBufferedRequests);
        }

        [TestMethod]
        public void WackySystemClock()
        {
            var senderId = "A";
            var receiverId = "B";

            var senderSorter = new MessageSorter();

            // simulate system clock that goes backwards - mechanism should still guarantee monotonicitty
            var message1 = Send(senderId, receiverId, "1", senderSorter, DateTime.UtcNow);
            var message2 = Send(senderId, receiverId, "2", senderSorter, DateTime.UtcNow - TimeSpan.FromSeconds(1));
            var message3 = Send(senderId, receiverId, "3", senderSorter, DateTime.UtcNow - TimeSpan.FromSeconds(2));

            List<RequestMessage> batch;
            MessageSorter receiverSorter = new MessageSorter();

            // delivering the sequence in order produces 1 message each time
            batch = receiverSorter.ReceiveInOrder(message1, ReorderWindow).ToList();
            Assert.That.Single(batch).Input.Equals("1");
            batch = receiverSorter.ReceiveInOrder(message2, ReorderWindow).ToList();
            Assert.That.Single(batch).Input.Equals("2");
            batch = receiverSorter.ReceiveInOrder(message3, ReorderWindow).ToList();
            Assert.That.Single(batch).Input.Equals("3");

            Assert.AreEqual(0, receiverSorter.NumberBufferedRequests);
        }

        [TestMethod]
        public void DelayedElement()
        {
            var senderId = "A";
            var receiverId = "B";

            var senderSorter = new MessageSorter();

            var message1 = Send(senderId, receiverId, "1", senderSorter, DateTime.UtcNow);
            var message2 = Send(senderId, receiverId, "2", senderSorter, DateTime.UtcNow);
            var message3 = Send(senderId, receiverId, "3", senderSorter, DateTime.UtcNow);

            List<RequestMessage> batch;
            MessageSorter receiverSorter;

            // delivering first message last delays all messages until getting the first one
            receiverSorter = new MessageSorter();
            batch = receiverSorter.ReceiveInOrder(message2, ReorderWindow).ToList();
            Assert.That.Empty(batch);
            batch = receiverSorter.ReceiveInOrder(message3, ReorderWindow).ToList();
            Assert.That.Empty(batch);
            batch = receiverSorter.ReceiveInOrder(message1, ReorderWindow).ToList();
            Assert.That.Collection(
                batch,
                first => first.Input.Equals("1"),
                second => second.Input.Equals("2"),
                third => third.Input.Equals("3"));

            Assert.AreEqual(0, receiverSorter.NumberBufferedRequests);
        }

        [TestMethod]
        public void NoFilteringOrSortingPastReorderWindow()
        {
            var senderId = "A";
            var receiverId = "B";

            var senderSorter = new MessageSorter();
            var now = DateTime.UtcNow;

            // last message is sent after an interval exceeding the reorder window
            var message1 = Send(senderId, receiverId, "1", senderSorter, now);
            var message2 = Send(senderId, receiverId, "2", senderSorter, now + TimeSpan.FromTicks(1));
            var message3 = Send(senderId, receiverId, "3", senderSorter, now + TimeSpan.FromTicks(2) + ReorderWindow);

            List<RequestMessage> batch;
            MessageSorter receiverSorter = new MessageSorter();

            // delivering the sequence in order produces 1 message each time
            receiverSorter = new MessageSorter();
            batch = receiverSorter.ReceiveInOrder(message1, ReorderWindow).ToList();
            Assert.That.Single(batch).Input.Equals("1");
            batch = receiverSorter.ReceiveInOrder(message2, ReorderWindow).ToList();
            Assert.That.Single(batch).Input.Equals("2");
            batch = receiverSorter.ReceiveInOrder(message3, ReorderWindow).ToList();
            Assert.That.Single(batch).Input.Equals("3");

            // duplicates are not filtered or sorted, but simply passed through, because we are past the reorder window
            batch = receiverSorter.ReceiveInOrder(message2, ReorderWindow).ToList();
            Assert.That.Single(batch).Input.Equals("2");
            batch = receiverSorter.ReceiveInOrder(message1, ReorderWindow).ToList();
            Assert.That.Single(batch).Input.Equals("1");

            Assert.AreEqual(0, receiverSorter.NumberBufferedRequests);
        }

        [TestMethod]
        public void DuplicatedElements()
        {
            var senderId = "A";
            var receiverId = "B";

            var senderSorter = new MessageSorter();

            var message1 = Send(senderId, receiverId, "1", senderSorter, DateTime.UtcNow);
            var message2 = Send(senderId, receiverId, "2", senderSorter, DateTime.UtcNow);
            var message3 = Send(senderId, receiverId, "3", senderSorter, DateTime.UtcNow);

            List<RequestMessage> batch;
            MessageSorter receiverSorter;

            // delivering first message last delays all messages until getting the first one
            receiverSorter = new MessageSorter();
            batch = receiverSorter.ReceiveInOrder(message2, ReorderWindow).ToList();
            Assert.That.Empty(batch);
            batch = receiverSorter.ReceiveInOrder(message2, ReorderWindow).ToList();
            Assert.That.Empty(batch);
            batch = receiverSorter.ReceiveInOrder(message1, ReorderWindow).ToList();
            Assert.That.Collection(
                batch,
                first => first.Input.Equals("1"),
                second => second.Input.Equals("2"));
            batch = receiverSorter.ReceiveInOrder(message2, ReorderWindow).ToList();
            Assert.That.Empty(batch);
            batch = receiverSorter.ReceiveInOrder(message1, ReorderWindow).ToList();
            Assert.That.Empty(batch);
            batch = receiverSorter.ReceiveInOrder(message3, ReorderWindow).ToList();
            Assert.That.Single(batch).Input.Equals("3");
            batch = receiverSorter.ReceiveInOrder(message3, ReorderWindow).ToList();
            Assert.That.Empty(batch);
            batch = receiverSorter.ReceiveInOrder(message2, ReorderWindow).ToList();
            Assert.That.Empty(batch);
            batch = receiverSorter.ReceiveInOrder(message1, ReorderWindow).ToList();
            Assert.That.Empty(batch);

            Assert.AreEqual(0, receiverSorter.NumberBufferedRequests);
        }

        [TestMethod]
        public void RandomShuffleAndDuplication()
        {
            var senderId = "A";
            var receiverId = "B";

            var senderSorter = new MessageSorter();
            var receiverSorter = new MessageSorter();

            var messageCount = 100;
            var duplicateCount = 100;

            // create a ordered sequence of messages
            var messages = new List<RequestMessage>();
            for (int i = 0; i < messageCount; i++)
            {
                messages.Add(Send(senderId, receiverId, i.ToString(), senderSorter, DateTime.UtcNow));
            }

            // add some random duplicates
            var random = new Random(0);
            for (int i = 0; i < duplicateCount; i++)
            {
                messages.Add(messages[random.Next(messageCount)]);
            }

            // shuffle the messages
            Shuffle(messages, random);

            // deliver all the messages
            var deliveredMessages = new List<RequestMessage>();

            foreach (var msg in messages)
            {
                foreach (var deliveredMessage in receiverSorter.ReceiveInOrder(msg, ReorderWindow))
                {
                    deliveredMessages.Add(deliveredMessage);
                }
            }

            // check that the delivered messages are the original sequence
            Assert.AreEqual(messageCount, deliveredMessages.Count());
            for (int i = 0; i < messageCount; i++)
            {
                Assert.AreEqual(i.ToString(), deliveredMessages[i].Input);
            }

            Assert.AreEqual(0, receiverSorter.NumberBufferedRequests);
        }

        /// <summary>
        /// Tests that if messages get reordered beyond the supported reorder window,
        /// we still deliver them all but they may now be out of order.
        /// </summary>
        [TestMethod]
        public void RandomCollection()
        {
            var senderId = "A";
            var receiverId = "B";

            var senderSorter = new MessageSorter();
            var receiverSorter = new MessageSorter();

            var messageCount = 100;

            var random = new Random(0);
            var now = DateTime.UtcNow;

            // create a ordered sequence of messages
            var messages = new List<RequestMessage>();
            for (int i = 0; i < messageCount; i++)
            {
                messages.Add(Send(senderId, receiverId, i.ToString(), senderSorter, now + TimeSpan.FromSeconds(random.Next(5)), TimeSpan.FromSeconds(10)));
            }

            // shuffle the messages
            Shuffle(messages, random);

            // add a final message
            messages.Add(Send(senderId, receiverId, (messageCount + 1).ToString(), senderSorter, now + TimeSpan.FromSeconds(1000), TimeSpan.FromSeconds(10)));

            // deliver all the messages
            var deliveredMessages = new List<RequestMessage>();

            for (int i = 0; i < messageCount; i++)
            {
                foreach (var deliveredMessage in receiverSorter.ReceiveInOrder(messages[i], TimeSpan.FromSeconds(10)))
                {
                    deliveredMessages.Add(deliveredMessage);
                }

                Assert.AreEqual(i + 1, deliveredMessages.Count + receiverSorter.NumberBufferedRequests);
            }

            // receive the final messages
            foreach (var deliveredMessage in receiverSorter.ReceiveInOrder(messages[messageCount], TimeSpan.FromSeconds(10)))
            {
                deliveredMessages.Add(deliveredMessage);
            }

            // check that all messages were delivered
            Assert.AreEqual(messageCount + 1, deliveredMessages.Count());

            Assert.AreEqual(0, receiverSorter.NumberBufferedRequests);
        }

        private static RequestMessage Send(string senderId, string receiverId, string input, MessageSorter sorter, DateTime now, TimeSpan? reorderWindow = null)
        {
            var msg = new RequestMessage()
            {
                Id = Guid.NewGuid(),
                ParentInstanceId = senderId,
                Input = input,
            };
            sorter.LabelOutgoingMessage(msg, receiverId, now, reorderWindow.HasValue ? reorderWindow.Value : ReorderWindow);
            return msg;
        }

        private static void Shuffle<T>(IList<T> list, Random random)
        {
            int n = list.Count;
            while (n > 1)
            {
                n--;
                int k = random.Next(n + 1);
                T value = list[k];
                list[k] = list[n];
                list[n] = value;
            }
        }
    }

    internal static class AssertExtensions
    {
        public static void Empty<T>(this Assert assert, IEnumerable<T> collection)
        {
            Assert.AreEqual(0, collection.Count());
        }

        public static T Single<T>(this Assert assert, IEnumerable<T> collection)
        {
            var e = collection.GetEnumerator();
            Assert.IsTrue(e.MoveNext());
            T element = e.Current;
            Assert.IsFalse(e.MoveNext());
            return element;
        }

        public static void Collection<T>(this Assert assert, IEnumerable<T> collection, params Action<T>[] elementInspectors)
        {
            var list = collection.ToList();
            Assert.AreEqual(elementInspectors.Length, list.Count);
            for(int i = 0; i < elementInspectors.Length; i++)
            {
                elementInspectors[i](list[i]);
            }
        }
    }
}