using Microsoft.Extensions.Logging;
using System;
#nullable enable
namespace DurableTask.Core.Logging
{
    /// <summary>
    /// Helper to write debug logs to trace at webjobs.worker.extensions level
    /// </summary>
    public class DebugHelper
    {
        readonly ILogger? log;

        /// <summary>
        /// Creates a new instance of the DebugHelper class
        /// </summary>
        public DebugHelper(ILogger? log)
        {
            // null is okay
            this.log = log;
        }

        /// <summary>
        /// Method to write OrchestrationDebugTrace logs to the ILogger. Debug uses only
        /// </summary>
        public void OrchestrationDebugTrace(string instanceId, string executionId, string details)
        {
            this.WriteStructuredLog(new LogEvents.OrchestrationDebugTrace(instanceId, executionId, details));
        }

        void WriteStructuredLog(ILogEvent logEvent, Exception? exception = null)
        {
            this.log?.LogDurableEvent(logEvent, exception);
        }


    }
}
