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

namespace DurableTask.Samples.Tests
{
    using System;
    using System.Threading.Tasks;
    using DurableTask.Samples.AverageCalculator;
    using DurableTask.Core.Test;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class AverageCalculatorTests
    {
        [TestMethod]
        public async Task AverageCalculatorBasicTest()
        {
            OrchestrationTestHost host = new OrchestrationTestHost();
            host.AddTaskOrchestrations(typeof (AverageCalculatorOrchestration))
                .AddTaskActivities(typeof (ComputeSumTask));

            var result = await host.RunOrchestration<double>(typeof(AverageCalculatorOrchestration), new int[] { 1, 50, 10 });

            Assert.AreEqual<double>(25, result);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentException))]
        public async Task AverageCalculatorWrongNumberOfArgs()
        {
            OrchestrationTestHost host = new OrchestrationTestHost();
            host.AddTaskOrchestrations(typeof(AverageCalculatorOrchestration));
            host.AddTaskActivities(typeof(ComputeSumTask));

            var result = await host.RunOrchestration<double>(typeof(AverageCalculatorOrchestration), new int[] { 1, 50 });

            Assert.AreEqual<double>(25, result);
        }
    }
}
