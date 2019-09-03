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

namespace DurableTask.AzureServiceFabric.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;

    using DurableTask.Core;
    using DurableTask.Core.History;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class PersistentSessionTests
    {
        [TestMethod]
        public void PersistentSession_SerializationTest()
        {
            int numberOfHistoryEvents = 256;

            var events = new List<HistoryEvent>();
            events.Add(new ExecutionStartedEvent(-1, "TestInput"));
            for (int i = 0; i < numberOfHistoryEvents; i++)
            {
                events.Add(new TaskScheduledEvent(-1));
                events.Add(new TaskCompletedEvent(-1, -1, $"Task {i} Result"));
            }
            events.Add(new ExecutionCompletedEvent(-1, "FinalResult", OrchestrationStatus.Completed));

            var instance = new OrchestrationInstance()
            {
                InstanceId = "testSession",
                ExecutionId = Guid.NewGuid().ToString("N")
            };

            PersistentSession testSession = PersistentSession.Create(instance, events.ToImmutableList());

            var actual = Measure.DataContractSerialization(testSession);

            Assert.IsNotNull(actual);
            Assert.AreEqual(instance.InstanceId, actual.SessionId.InstanceId);
            Assert.AreEqual(instance.ExecutionId, actual.SessionId.ExecutionId);
            Assert.AreEqual(numberOfHistoryEvents * 2 + 2, actual.SessionState.Count);
        }
    }
}
