//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace DurableTaskSamplesUnitTests
{
    using System;
    using System.Threading.Tasks;
    using DurableTask.Test;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using DurableTaskSamples.AverageCalculator;

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
