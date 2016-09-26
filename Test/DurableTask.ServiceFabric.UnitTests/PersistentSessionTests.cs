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
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using DurableTask.History;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DurableTask.ServiceFabric.UnitTests
{
    [TestClass]
    public class PersistentSessionTests
    {
        [TestMethod]
        public void SerializationTest()
        {
            int numberOfItemsInCollections = 2;

            var events = new List<HistoryEvent>();
            events.Add(new ExecutionStartedEvent(-1, "TestInput"));
            for (int i = 0; i < numberOfItemsInCollections; i++)
            {
                events.Add(new TaskScheduledEvent(-1));
                events.Add(new TaskCompletedEvent(-1, -1, $"Task {i} Result"));
            }
            events.Add(new ExecutionCompletedEvent(-1, "FinalResult", OrchestrationStatus.Completed));

            var messages = new List<ReceivableTaskMessage>();
            for (int i = 0; i < numberOfItemsInCollections; i++)
            {
                messages.Add(ReceivableTaskMessage.Create(new TaskMessage() { SequenceNumber = i }));
            }

            var scheduledMessages = new List<ReceivableTaskMessage>();
            for (int i = 0; i < numberOfItemsInCollections; i++)
            {
                scheduledMessages.Add(ReceivableTaskMessage.Create(new TaskMessage() { SequenceNumber = i }));
            }

            PersistentSession testSession = PersistentSession.Create("testSession", events.ToImmutableList(),
                messages.ToImmutableList(), scheduledMessages.ToImmutableList(), isLocked: true);

            using (var stream = new MemoryStream())
            {
                var serializer = new DataContractSerializer(typeof(PersistentSession));
                Stopwatch timer = Stopwatch.StartNew();
                serializer.WriteObject(stream, testSession);
                timer.Stop();
                Console.WriteLine($"Time for serialization : {timer.ElapsedMilliseconds} ms");

                stream.Position = 0;
                timer.Restart();
                var deserialized = (PersistentSession)serializer.ReadObject(stream);
                timer.Stop();
                Console.WriteLine($"Time for deserialization : {timer.ElapsedMilliseconds} ms");

                Assert.IsNotNull(deserialized);
                Assert.AreEqual("testSession", deserialized.SessionId);
                Assert.IsFalse(deserialized.IsLocked);
                Assert.AreEqual(numberOfItemsInCollections*2 + 2, deserialized.SessionState.Count);
                Assert.AreEqual(numberOfItemsInCollections, deserialized.Messages.Count);
                Assert.AreEqual(numberOfItemsInCollections, deserialized.ScheduledMessages.Count);
            }
        }
    }
}
