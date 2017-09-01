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

using System.Text;

namespace DurableTask.AzureStorage.Monitoring
{
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
        public int AggregateControlQueueLength { get; internal set; }

        /// <summary>
        /// Gets a trend value describing the number of messages in the control queues over a period of time.
        /// </summary>
        public double AggregateControlQueueLengthTrend { get; internal set; }

        /// <summary>
        /// Gets the number of messages in the work-item queue.
        /// </summary>
        public int WorkItemQueueLength { get; internal set; }

        /// <summary>
        /// Gets a trend value describing the number of messages in the work-item queue over a period of time.
        /// </summary>
        public double WorkItemQueueLengthTrend { get; internal set; }

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
            sb.Append(nameof(this.AggregateControlQueueLength)).Append(": ").Append(this.AggregateControlQueueLength).Append(", ");
            sb.Append(nameof(this.AggregateControlQueueLengthTrend)).Append(": ").Append(this.AggregateControlQueueLengthTrend).Append(", ");
            sb.Append(nameof(this.WorkItemQueueLength)).Append(": ").Append(this.WorkItemQueueLength).Append(", ");
            sb.Append(nameof(this.WorkItemQueueLengthTrend)).Append(": ").Append(this.WorkItemQueueLengthTrend).Append(", ");
            sb.Append(nameof(this.ScaleRecommendation)).Append(": ").Append(this.ScaleRecommendation);
            return sb.ToString();
        }
    }
}
