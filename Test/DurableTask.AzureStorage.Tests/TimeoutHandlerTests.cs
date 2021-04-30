using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DurableTask.AzureStorage.Tests
{
    /// <summary>
    /// Tests for <see cref="TimeoutHandler"/>.
    /// </summary>
    [TestClass]
    public class TimeoutHandlerTests
    {
        /// <summary>
        /// Ensures that process graceful action is executed before process is killed.
        /// </summary>
        /// <returns>Task tracking operation.</returns>
        [TestMethod]
        public async Task EnsureTimeoutHandlerRunsProcessShutdownEventsBeforeProcessKill()
        {
            int executionCount = 0;
            int killCount = 0;
            int shutdownCount = 0;

            Action<string> killAction = (errorString) => killCount++;
            typeof(TimeoutHandler)
                .GetField("ProcessKillAction", BindingFlags.NonPublic | BindingFlags.Static)
                .SetValue(null, killAction);

            // TimeoutHandler at the moment invokes shutdown on 5th call failure.
            await TimeoutHandler.ExecuteWithTimeout(
                "test",
                "account",
                new AzureStorageOrchestrationServiceSettings
                {
                    ProcessGracefulShutdownAction = (errorString) =>
                    {
                        shutdownCount++;
                        return Task.CompletedTask;
                    }
                },
                async (operationContext, cancellationToken) =>
                {
                    executionCount++;
                    await Task.Delay(TimeSpan.FromMinutes(3));
                    return 1;
                });

            Assert.AreEqual(5, executionCount);
            Assert.AreEqual(1, shutdownCount);
            Assert.AreEqual(1, killCount);
        }
    }
}
