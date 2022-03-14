using System;
using System.Diagnostics;
using Newtonsoft.Json;

namespace DurableTask.Core
{
    /// <summary>
    /// Manage Activity for orchestration execution.
    /// </summary>
    public class DistributedTraceActivity
    {
        /// <summary>
        /// Share the Activity across an orchestration execution.
        /// </summary>
        public static Activity Current { get; set; }
    }
}
