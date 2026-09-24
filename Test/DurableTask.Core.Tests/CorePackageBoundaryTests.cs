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
    using System.Linq;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class CorePackageBoundaryTests
    {
        [TestMethod]
        [DataRow("DurableTask.Core.IOrchestrationServiceLargePayloadPurgeClient")]
        [DataRow("DurableTask.Core.LargePayloadPurgeTombstone")]
        [DataRow("DurableTask.Core.LargePayloadPurgeResult")]
        [DataRow("DurableTask.Core.LargePayloadPurgeDisposition")]
        public void CoreDoesNotDefineOrForwardPurgeContracts(string typeName)
        {
            var assembly = typeof(TaskHubClient).Assembly;

            // GetType resolves forwarded types as well as types defined in this assembly.
            Assert.IsNull(assembly.GetType(typeName));
        }

        [TestMethod]
        public void CoreDoesNotReferencePurgeAbstractionsOrSdkClient()
        {
            string[] references = typeof(TaskHubClient).Assembly.GetReferencedAssemblies()
                .Select(assembly => assembly.Name).ToArray();

            CollectionAssert.DoesNotContain(references, "DurableTask.LargePayloadPurge.Abstractions");
            CollectionAssert.DoesNotContain(references, "Microsoft.DurableTask.Client");
        }
    }
}
