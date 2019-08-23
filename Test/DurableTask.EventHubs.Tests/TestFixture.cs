using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.EventSourced.Tests
{
    public class TestFixture : IDisposable
    {
        public TestFixture()
        {
            this.Host = TestHelpers.GetTestOrchestrationHost(EnableExtendedSessions);
            this.Host.StartAsync().Wait();
        }

        internal const bool EnableExtendedSessions = false;

        public void Dispose()
        {
            this.Host.StopAsync().Wait();
            this.Host.Dispose();
        }

        internal TestOrchestrationHost Host { get; private set; }
    }

}
