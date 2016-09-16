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

using System.Collections.Immutable;
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
            };

            PersistentSession testSession = PersistentSession.Create(sessionId: "testSession", sessionState: events.ToImmutableList());

            using (var stream = new MemoryStream())
            {
                var serializer = new DataContractSerializer(typeof(PersistentSession));
                serializer.WriteObject(stream, testSession);

                stream.Position = 0;
                var deserialized = (PersistentSession)serializer.ReadObject(stream);

                Assert.IsNotNull(deserialized);
                Assert.AreEqual("testSession", deserialized.SessionId);
                Assert.AreEqual(4, deserialized.SessionState.Count);
            }
        }
    }
}
