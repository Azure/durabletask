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

using DurableTask.History;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DurableTask.ServiceFabric.UnitTests
{
    [TestClass]
    public class TaskMessageItemTests
    {
        [TestMethod]
        public void TaskMessageItem_SerializationTests()
        {
            var expected = new TaskMessageItem(new TaskMessage()
            {
                OrchestrationInstance = new OrchestrationInstance() { InstanceId = "InstanceId", ExecutionId = "ExecutionId" },
                SequenceNumber = 33,
                Event = new TaskScheduledEvent(-1)
            });

            var actual = Measure.DataContractSerialization(expected);

            Assert.IsNotNull(actual);
            Assert.AreEqual(expected.Message.OrchestrationInstance.InstanceId, actual.Message.OrchestrationInstance.InstanceId);
            Assert.AreEqual(expected.Message.OrchestrationInstance.ExecutionId, actual.Message.OrchestrationInstance.ExecutionId);
            Assert.AreEqual(expected.Message.SequenceNumber, actual.Message.SequenceNumber);
            Assert.AreEqual(expected.Message.Event.EventId, actual.Message.Event.EventId);
        }
    }
}
