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

namespace DurableTask.LargePayloadPurge.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.DurableTask.Client;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class LargePayloadPurgeContractTests
    {
        [TestMethod]
        public void PackageExportsOnlyTheStandaloneInterface()
        {
            Type contract = typeof(IOrchestrationServiceLargePayloadPurgeClient);

            Assert.IsTrue(contract.IsInterface);
            Assert.AreEqual("DurableTask.LargePayloadPurge", contract.Namespace);
            Assert.AreEqual("DurableTask.LargePayloadPurge.Abstractions", contract.Assembly.GetName().Name);
            CollectionAssert.AreEqual(new[] { contract }, contract.Assembly.GetExportedTypes());
            Assert.AreEqual(0, contract.GetInterfaces().Length);
            Assert.AreEqual(3, contract.GetMethods().Length);
        }

        [TestMethod]
        public void SetAcceptsExplicitChoiceAndCallerDeadlineAndCancellation()
        {
            AssertSignature(
                nameof(IOrchestrationServiceLargePayloadPurgeClient.SetLargePayloadAutoPurgeAsync),
                typeof(Task),
                new[] { typeof(bool), typeof(DateTime), typeof(CancellationToken) },
                new[] { "enabled", "deadlineUtc", "cancellationToken" });
        }

        [TestMethod]
        public void GetReturnsCanonicalSdkTombstones()
        {
            AssertSignature(
                nameof(IOrchestrationServiceLargePayloadPurgeClient.GetLargePayloadsToPurgeAsync),
                typeof(Task<IReadOnlyList<LargePayloadTombstone>>),
                new[] { typeof(int), typeof(DateTime), typeof(CancellationToken) },
                new[] { "limit", "deadlineUtc", "cancellationToken" });
        }

        [TestMethod]
        public void ReportAcceptsCanonicalSdkResults()
        {
            AssertSignature(
                nameof(IOrchestrationServiceLargePayloadPurgeClient.ReportLargePayloadPurgeResultsAsync),
                typeof(Task),
                new[] { typeof(IReadOnlyList<LargePayloadPurgeResult>), typeof(DateTime), typeof(CancellationToken) },
                new[] { "results", "deadlineUtc", "cancellationToken" });
        }

        [TestMethod]
        public void ModelsComeFromSdkClientNotTheInterfacePackage()
        {
            Assembly clientAssembly = typeof(DurableTaskClient).Assembly;

            Assert.AreSame(clientAssembly, typeof(LargePayloadTombstone).Assembly);
            Assert.AreSame(clientAssembly, typeof(LargePayloadPurgeResult).Assembly);
            Assert.AreSame(clientAssembly, typeof(LargePayloadPurgeDisposition).Assembly);
            Assert.AreNotSame(clientAssembly, typeof(IOrchestrationServiceLargePayloadPurgeClient).Assembly);
        }

        static void AssertSignature(string methodName, Type returnType, Type[] parameterTypes, string[] parameterNames)
        {
            MethodInfo method = typeof(IOrchestrationServiceLargePayloadPurgeClient).GetMethod(methodName);
            Assert.IsNotNull(method);
            Assert.AreEqual(returnType, method.ReturnType);
            ParameterInfo[] parameters = method.GetParameters();
            CollectionAssert.AreEqual(parameterTypes, parameters.Select(parameter => parameter.ParameterType).ToArray());
            CollectionAssert.AreEqual(parameterNames, parameters.Select(parameter => parameter.Name).ToArray());
            Assert.IsFalse(parameters.Any(parameter => parameter.IsOptional));
        }
    }
}
