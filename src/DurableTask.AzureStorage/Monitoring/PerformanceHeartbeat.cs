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

namespace DurableTask.AzureStorage.Monitoring
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    /// <summary>
    /// Data structure containing point-in-time performance metrics for a durable task hub.
    /// </summary>
    public class PerformanceHeartbeat
    {
        /// <summary>
        /// Gets the number of partitions configured in the task hub.
        /// </summary>
        public int PartitionCount { get; internal set; }

        /// <summary>
        /// Gets the number of messages across all control queues.
        /// </summary>
        public IReadOnlyList<int> ControlQueueLengths { get; internal set; }

        /// <summary>
        /// Gets the latency of messages in all control queues.
        /// </summary>
        public IReadOnlyList<TimeSpan> ControlQueueLatencies { get; internal set; }

        /// <summary>
        /// Gets the number of messages in the work-item queue.
        /// </summary>
        public int WorkItemQueueLength { get; internal set; }

        /// <summary>
        /// Gets a trend value describing the latency of messages in the work-item queue over a period of time.
        /// </summary>
        public double WorkItemQueueLatencyTrend { get; internal set; }

        /// <summary>
        /// Gets the approximate age of the first work-item queue message.
        /// </summary>
        public TimeSpan WorkItemQueueLatency { get; internal set; }

        /// <summary>
        /// Gets a scale recommendation for the task hub given the current performance metrics.
        /// </summary>
        public ScaleRecommendation ScaleRecommendation { get; internal set; }

        /// <summary>
        /// Gets a string description of the current <see cref="PerformanceHeartbeat"/> object.
        /// </summary>
        /// <returns>A string description useful for diagnostics.</returns>
        public override string ToString()
        {
            var sb = new StringBuilder(1024);
            sb.Append(nameof(this.PartitionCount)).Append(": ").Append(this.PartitionCount).Append(", ");
            if (this.ControlQueueLengths != null)
            {
                sb.Append(nameof(this.ControlQueueLengths)).Append(": ").Append(string.Join(",", this.ControlQueueLengths)).Append(", ");
            }

            sb.Append(nameof(this.ControlQueueLatencies)).Append(": ").Append(string.Join(",", this.ControlQueueLatencies)).Append(", ");
            sb.Append(nameof(this.WorkItemQueueLength)).Append(": ").Append(this.WorkItemQueueLength).Append(", ");
            sb.Append(nameof(this.WorkItemQueueLatency)).Append(": ").Append(this.WorkItemQueueLatency).Append(", ");
            sb.Append(nameof(this.WorkItemQueueLatencyTrend)).Append(": ").Append(this.WorkItemQueueLatencyTrend).Append(", ");
            sb.Append(nameof(this.ScaleRecommendation)).Append(": ").Append(this.ScaleRecommendation);
            return sb.ToString();
        }
    }
}
