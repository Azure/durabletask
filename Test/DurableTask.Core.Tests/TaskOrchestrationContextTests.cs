// ---------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ---------------------------------------------------------------

namespace DurableTask.Core.Tests
{
    using DurableTask.Core.Command;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    [TestClass]
    public class TaskOrchestrationContextTests
    {
        [DataTestMethod]
        public async Task CreateTimerAlwaysUsesUtcTime()
        {
            var originalTime = DateTime.UtcNow;

            var context = new TaskOrchestrationContext(new OrchestrationInstance(), null);
            var cts = new CancellationTokenSource();
            cts.Cancel();
            try
            {
                await context.CreateTimer<bool>(originalTime, false, cts.Token);
            }
            catch (TaskCanceledException)
            {
                // Expected
            }

            var resultTime = ((CreateTimerOrchestratorAction)context.OrchestratorActions.Single()).FireAt;
            Assert.AreEqual(DateTimeKind.Utc, resultTime.Kind);
            Assert.AreEqual(originalTime, resultTime);
        }

        [DataTestMethod]
        [DataRow(DateTimeKind.Local)]
        [DataRow(DateTimeKind.Unspecified)]
        public async Task CreateTimerThrowsOnNonUtcTime(DateTimeKind kind)
        {
            var originalTime = new DateTime(1, kind);

            var context = new TaskOrchestrationContext(new OrchestrationInstance(), null);

            await Assert.ThrowsExceptionAsync<ArgumentException>(async () => await context.CreateTimer<bool>(originalTime, false));
        }
    }
}
