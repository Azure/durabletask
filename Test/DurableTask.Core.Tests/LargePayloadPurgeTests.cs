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
    using DurableTask.Emulator;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class LargePayloadPurgeTests
    {
        [TestMethod]
        [DataRow("tombstone-token", "payload-token")]
        [DataRow("", "")]
        [DataRow(" \t\r\n ", "\t ")]
        [DataRow("Case/Sensitive+Token=", "%2Fopaque+payload==")]
        public void TombstonePreservesOpaqueTokens(string tombstoneToken, string payloadToken)
        {
            var tombstone = new LargePayloadPurgeTombstone(tombstoneToken, payloadToken);

            Assert.AreEqual(tombstoneToken, tombstone.TombstoneToken);
            Assert.AreEqual(payloadToken, tombstone.PayloadToken);
        }

        [TestMethod]
        [DataRow(null, "payload", "tombstoneToken")]
        [DataRow("tombstone", null, "payloadToken")]
        [DataRow(null, null, "tombstoneToken")]
        public void TombstoneRejectsNullTokens(string tombstoneToken, string payloadToken, string parameterName)
        {
            ArgumentNullException exception = Assert.ThrowsException<ArgumentNullException>(
                () => new LargePayloadPurgeTombstone(tombstoneToken, payloadToken));

            Assert.AreEqual(parameterName, exception.ParamName);
        }

        [TestMethod]
        [DataRow("", 0)]
        [DataRow(" \t\r\n ", 1)]
        [DataRow("Case/Sensitive+Token=", 2)]
        [DataRow("tombstone-token", 3)]
        [DataRow("tombstone-token", -1)]
        [DataRow("tombstone-token", 4)]
        [DataRow("tombstone-token", int.MaxValue)]
        public void ResultPreservesOpaqueTokenAndUnvalidatedDisposition(string tombstoneToken, int disposition)
        {
            var result = new LargePayloadPurgeResult(tombstoneToken, (LargePayloadPurgeDisposition)disposition);

            Assert.AreEqual(tombstoneToken, result.TombstoneToken);
            Assert.AreEqual(disposition, (int)result.Disposition);
        }

        [TestMethod]
        public void ResultRejectsNullToken()
        {
            ArgumentNullException exception = Assert.ThrowsException<ArgumentNullException>(
                () => new LargePayloadPurgeResult(null, LargePayloadPurgeDisposition.Deleted));

            Assert.AreEqual("tombstoneToken", exception.ParamName);
        }

        [TestMethod]
        public void DispositionValuesRetainNumericContract()
        {
            Assert.AreEqual(0, (int)LargePayloadPurgeDisposition.Unspecified);
            Assert.AreEqual(1, (int)LargePayloadPurgeDisposition.Deleted);
            Assert.AreEqual(2, (int)LargePayloadPurgeDisposition.Retry);
            Assert.AreEqual(3, (int)LargePayloadPurgeDisposition.Quarantined);
        }

        [TestMethod]
        public void ModelsRetainReferenceEquality()
        {
            var tombstone = new LargePayloadPurgeTombstone("tombstone", "payload");
            var result = new LargePayloadPurgeResult("tombstone", LargePayloadPurgeDisposition.Deleted);

            Assert.AreNotEqual(tombstone, new LargePayloadPurgeTombstone("tombstone", "payload"));
            Assert.AreNotEqual(result, new LargePayloadPurgeResult("tombstone", LargePayloadPurgeDisposition.Deleted));
        }

        [TestMethod]
        public async Task ExistingServiceClientWorksWithoutOptionalLargePayloadPurgeCapability()
        {
            using var service = new LocalOrchestrationService();
            IOrchestrationServiceClient serviceClient = service;
            Assert.IsFalse(serviceClient is IOrchestrationServiceLargePayloadPurgeClient);

            var client = new TaskHubClient(serviceClient);
            OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(
                "Business.Orchestration", string.Empty, input: null);
            OrchestrationState state = await client.GetOrchestrationStateAsync(instance);

            Assert.AreEqual(instance.InstanceId, state.OrchestrationInstance.InstanceId);
            Assert.AreEqual("Business.Orchestration", state.Name);
            Assert.AreEqual(OrchestrationStatus.Pending, state.OrchestrationStatus);
        }
    }
}
