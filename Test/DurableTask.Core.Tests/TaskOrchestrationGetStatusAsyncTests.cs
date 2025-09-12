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

namespace DurableTask.Core.Tests
{
    using System;
    using System.Threading.Tasks;
    using DurableTask.Core.Serializing;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class TaskOrchestrationGetStatusAsyncTests
    {
        [TestMethod]
        public async Task GetStatusAsync_ReturnsSerializedStatus_FromTypedOrchestration()
        {
            var orchestration = new SampleTypedOrchestration();

            string syncStatus = orchestration.GetStatus();
            string asyncStatus = await orchestration.GetStatusAsync();

            Assert.AreEqual(syncStatus, asyncStatus, "Async status should equal sync status");

            var deserialized = (Status)orchestration.DataConverter.Deserialize(asyncStatus, typeof(Status));
            Assert.AreEqual("running", deserialized.State);
            Assert.AreEqual(42, deserialized.Progress);
        }

        [TestMethod]
        public async Task GetStatusAsync_ReturnsNull_WhenTypedOnGetStatusReturnsNull()
        {
            var orchestration = new NullStatusTypedOrchestration();

            string syncStatus = orchestration.GetStatus();
            string asyncStatus = await orchestration.GetStatusAsync();

            Assert.IsNull(syncStatus, "Sync status should be null when OnGetStatus returns null");
            Assert.IsNull(asyncStatus, "Async status should be null when OnGetStatus returns null");
        }

        [TestMethod]
        public async Task GetStatusAsync_ReturnsSameAsGetStatus_ForNonGenericOrchestration()
        {
            var orchestration = new NonGenericOrchestration();

            string syncStatus = orchestration.GetStatus();
            string asyncStatus = await orchestration.GetStatusAsync();

            Assert.AreEqual("OK", syncStatus);
            Assert.AreEqual(syncStatus, asyncStatus);
        }

        [TestMethod]
        public async Task GetStatusAsync_PropagatesException_WhenGetStatusThrows()
        {
            var orchestration = new ThrowingStatusOrchestration();

            await Assert.ThrowsExceptionAsync<InvalidOperationException>(async () => await orchestration.GetStatusAsync());
        }

        class SampleTypedOrchestration : TaskOrchestration<string, string, string, Status>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                return Task.FromResult("done");
            }

            public override Status OnGetStatus()
            {
                return new Status { State = "running", Progress = 42 };
            }
        }

        class NullStatusTypedOrchestration : TaskOrchestration<string, string, string, Status>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                return Task.FromResult("done");
            }

            public override Status OnGetStatus()
            {
                return null;
            }
        }

        class NonGenericOrchestration : TaskOrchestration
        {
            public override Task<string> Execute(OrchestrationContext context, string input)
            {
                return Task.FromResult("done");
            }

            public override void RaiseEvent(OrchestrationContext context, string name, string input)
            {
            }

            public override string GetStatus()
            {
                return "OK";
            }
        }

        class ThrowingStatusOrchestration : TaskOrchestration
        {
            public override Task<string> Execute(OrchestrationContext context, string input)
            {
                return Task.FromResult("done");
            }

            public override void RaiseEvent(OrchestrationContext context, string name, string input)
            {
            }

            public override string GetStatus()
            {
                throw new InvalidOperationException("boom");
            }
        }

        class Status
        {
            public string State { get; set; }

            public int Progress { get; set; }
        }
    }
}

