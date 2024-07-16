// ---------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ---------------------------------------------------------------

namespace DurableTask.Core.Tests
{
    using DurableTask.Core.Command;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Linq;

    [TestClass]
    public class TaskOrchestrationContextTests
    {
        [DataTestMethod]
        [DataRow(DateTimeKind.Utc)]
        [DataRow(DateTimeKind.Local)]
        [DataRow(DateTimeKind.Unspecified)]
        public void CreateTimerAlwaysUsesUtcTime(DateTimeKind kind)
        {
            var originalTime = new DateTime(1, kind);

            var context = new TaskOrchestrationContext(new OrchestrationInstance(), null);
            var timer = context.CreateTimer<bool>(originalTime, false);

            var resultTime = ((CreateTimerOrchestratorAction)context.OrchestratorActions.Single()).FireAt;
            Assert.AreEqual(originalTime.ToUniversalTime(), resultTime);
        }
    }
}
