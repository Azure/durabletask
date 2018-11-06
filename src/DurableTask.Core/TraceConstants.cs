using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.Core
{
    /// <summary>
    /// TraceConstants for Distributed Tracing
    /// </summary>
    public static class TraceConstants
    {
        /// <summary>
        /// Client is the Distributed Tracing message for OrchestratorClient.
        /// </summary>
        public const string Client = "DtClient";

        /// <summary>
        /// Orchestrator is the Distributed Tracing message for Orchestrator.
        /// </summary>
        public const string Orchestrator = "DtOrchestrator";

        /// <summary>
        /// Activity is the Distributed Tracing message for Activity
        /// </summary>
        public const string Activity = "DtActivity";

        /// <summary>
        /// DependencyDefault is the Distributed Tracing message default name for Dependency.
        /// </summary>
        public const string DependencyDefault = "outbound";

    }
}
