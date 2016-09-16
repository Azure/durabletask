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
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Runtime.Serialization;
using DurableTask.History;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DurableTask.ServiceFabric.UnitTests
{
    [TestClass]
    public class PersistentSessionTests
    {
        [TestMethod]
        [Ignore] //Todo
        public void SerializationTest()
        {
            var events = new HistoryEvent[]
            {
                new ExecutionStartedEvent(-1, "TestInput"),
                new TaskScheduledEvent(-1),
                new TaskCompletedEvent(-1, -1, "SomeResult"),
                new ExecutionCompletedEvent(-1, "SomeResult", OrchestrationStatus.Completed)
            }.ToImmutableList();

            var messages = new ReceivableTaskMessage[]
            {
                ReceivableTaskMessage.Create(new TaskMessage() {SequenceNumber = 1}),
                ReceivableTaskMessage.Create(new TaskMessage() {SequenceNumber = 2}),
            }.ToImmutableList();

            var scheduledMessages = new ReceivableTaskMessage[]
            {
                ReceivableTaskMessage.Create(new TaskMessage() {SequenceNumber = 3}),
            }.ToImmutableList();

            PersistentSession testSession = PersistentSession.Create("testSession", events, messages, scheduledMessages, isLocked: true);

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
                Assert.AreEqual(4, deserialized.SessionState.Count);
                Assert.AreEqual(2, deserialized.Messages.Count);
                Assert.AreEqual(1, deserialized.ScheduledMessages.Count);
            }
        }
    }
}
