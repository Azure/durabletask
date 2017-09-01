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
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Queue;

    /// <summary>
    /// Utility class for collecting performance information for a Durable Task hub without actually running inside a Durable Task worker.
    /// </summary>
    public class DisconnectedPerformanceMonitor
    {
        internal const int QueueLengthSampleSize = 5;
        internal const int MaxMessagesPerWorkerRatio = 100;

        readonly QueueLengthHistory controlQueueLengths = new QueueLengthHistory(QueueLengthSampleSize);
        readonly QueueLengthHistory workItemQueueLengths = new QueueLengthHistory(QueueLengthSampleSize);

        readonly CloudStorageAccount storageAccount;
        readonly string taskHub;

        /// <summary>
        /// Initializes a new instance of the <see cref="DisconnectedPerformanceMonitor"/> class.
        /// </summary>
        /// <param name="storageConnectionString">The connection string for the Azure Storage account to monitor.</param>
        /// <param name="taskHub">The name of the task hub within the specified storage account.</param>
        public DisconnectedPerformanceMonitor(string storageConnectionString, string taskHub)
            : this(CloudStorageAccount.Parse(storageConnectionString), taskHub)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DisconnectedPerformanceMonitor"/> class.
        /// </summary>
        /// <param name="storageAccount">The Azure Storage account to monitor.</param>
        /// <param name="taskHub">The name of the task hub within the specified storage account.</param>
        public DisconnectedPerformanceMonitor(CloudStorageAccount storageAccount, string taskHub)
        {
            this.storageAccount = storageAccount;
            this.taskHub = taskHub;
        }

        /// <summary>
        /// Collects and returns a sampling of all performance metrics being observed by this instance.
        /// </summary>
        /// <param name="currentWorkerCount">The number of workers known to be processing messages for this task hub.</param>
        /// <returns>Returns a performance data summary or <c>null</c> if data cannot be obtained.</returns>
        public virtual async Task<PerformanceHeartbeat> PulseAsync(int currentWorkerCount)
        {
            int workItemQueueLength;
            int aggregateControlQueueLength;
            int partitionCount;

            try
            {
                ControlQueueData controlQueueData = await this.GetAggregateControlQueueLengthAsync();
                aggregateControlQueueLength = controlQueueData.AggregateQueueLength;
                partitionCount = controlQueueData.PartitionCount;
                workItemQueueLength = await this.GetWorkItemQueueLengthAsync();
            }
            catch (StorageException e) when (e.RequestInformation?.HttpStatusCode == 404)
            {
                // The queues are not yet provisioned.
                return null;
            }

            this.AddWorkItemQueueLength(workItemQueueLength);
            this.AddControlQueueLength(aggregateControlQueueLength);

            var heartbeatPayload = new PerformanceHeartbeat
            {
                PartitionCount = partitionCount,
                WorkItemQueueLength = workItemQueueLength,
                WorkItemQueueLengthTrend = this.workItemQueueLengths.CurrentTrend,
                AggregateControlQueueLength = aggregateControlQueueLength,
                AggregateControlQueueLengthTrend = this.controlQueueLengths.CurrentTrend,
                ScaleRecommendation = this.MakeScaleRecommendation(partitionCount, currentWorkerCount),
            };

            return heartbeatPayload;
        }

        /// <summary>
        /// Gets the approximate length of the work-item queue.
        /// </summary>
        /// <returns>The approximate number of messages in the work-item queue.</returns>
        protected virtual async Task<int> GetWorkItemQueueLengthAsync()
        {
            CloudQueue workItemQueue = AzureStorageOrchestrationService.GetWorkItemQueue(this.storageAccount, this.taskHub);
            await workItemQueue.FetchAttributesAsync();
            return workItemQueue.ApproximateMessageCount.GetValueOrDefault(0);
        }

        /// <summary>
        /// Gets the approximate aggreate length (sum) of the all known control queues.
        /// </summary>
        /// <returns>The approximate number of messages across all control queues.</returns>
        protected virtual async Task<ControlQueueData> GetAggregateControlQueueLengthAsync()
        {
            CloudQueue[] controlQueues = await AzureStorageOrchestrationService.GetControlQueuesAsync(
                this.storageAccount,
                this.taskHub,
                AzureStorageOrchestrationServiceSettings.DefaultPartitionCount);

            // There is one queue per partition.
            var result = new ControlQueueData();
            result.PartitionCount = controlQueues.Length;

            // We treat all control queues like one big queue and sum the lengths together.
            foreach (CloudQueue queue in controlQueues)
            {
                await queue.FetchAttributesAsync();
                int queueLength = queue.ApproximateMessageCount.GetValueOrDefault(0);
                result.AggregateQueueLength += queueLength;
            }

            return result;
        }

        /// <summary>
        /// Adds a work-item queue length sampling to the current monitor.
        /// </summary>
        /// <param name="queueLength">The approximate length of the work-item queue.</param>
        protected virtual void AddWorkItemQueueLength(int queueLength)
        {
            this.workItemQueueLengths.Add(queueLength);
        }

        /// <summary>
        /// Adds an aggregate control queue length sampling to the current monitor.
        /// </summary>
        /// <param name="aggregateQueueLength">The sum of all the control queue lengths.</param>
        protected virtual void AddControlQueueLength(int aggregateQueueLength)
        {
            this.controlQueueLengths.Add(aggregateQueueLength);
        }

        ScaleRecommendation MakeScaleRecommendation(int partitionCount, int workerCount)
        {
            ScaleAction pendingVote = ScaleAction.None;
            bool noWorkItems = false;

            if (workerCount == 0 && !(this.workItemQueueLengths.IsAllZeros() && this.controlQueueLengths.IsAllZeros()))
            {
                return new ScaleRecommendation(ScaleAction.AddWorker, keepWorkersAlive: true, reason: "First worker");
            }

            // Wait until we have enough samples before making specific recommendations
            if (!this.workItemQueueLengths.IsFull || !this.controlQueueLengths.IsFull)
            {
                bool keepWorkersAlive = !this.workItemQueueLengths.IsAllZeros() || !this.controlQueueLengths.IsAllZeros();
                ScaleAction scaleAction = workerCount == 0 && keepWorkersAlive ? ScaleAction.AddWorker : ScaleAction.None;
                return new ScaleRecommendation(scaleAction, keepWorkersAlive, reason: "Not enough samples");
            }

            // Look at the work item queue growth rate
            if (workerCount > 0 && this.workItemQueueLengths.Last > MaxMessagesPerWorkerRatio / workerCount)
            {
                return new ScaleRecommendation(ScaleAction.AddWorker, keepWorkersAlive: true, reason: $"Work items per worker > {MaxMessagesPerWorkerRatio}");
            }
            else if (this.workItemQueueLengths.IsTrendingUpwards())
            {
                return new ScaleRecommendation(ScaleAction.AddWorker, keepWorkersAlive: true, reason: "Work items increasing");
            }
            else if (this.workItemQueueLengths.IsTrendingDownwards())
            {
                noWorkItems = false;
                pendingVote = ScaleAction.RemoveWorker;
            }
            else if (this.workItemQueueLengths.IsAllZeros())
            {
                noWorkItems = true;
                pendingVote = ScaleAction.RemoveWorker;
            }

            // Look at the control queue growth rate
            // NOTE: The logic here assumes even distribution across all control queues.
            if (workerCount > 0 && workerCount < partitionCount && this.controlQueueLengths.Last > MaxMessagesPerWorkerRatio / workerCount)
            {
                return new ScaleRecommendation(ScaleAction.AddWorker, keepWorkersAlive: true, reason: $"Control items per worker > {MaxMessagesPerWorkerRatio}");
            }
            else if (workerCount < partitionCount && this.controlQueueLengths.IsTrendingUpwards())
            {
                return new ScaleRecommendation(ScaleAction.AddWorker, keepWorkersAlive: true, reason: "Control items increasing");
            }
            else if (pendingVote == ScaleAction.RemoveWorker)
            {
                if (this.controlQueueLengths.IsTrendingDownwards())
                {
                    string reason = noWorkItems ? "No work items; control items decreasing" : "Both work items and control items decreasing";
                    ScaleAction downscaleAction = workerCount > 0 ? ScaleAction.RemoveWorker : ScaleAction.None;
                    return new ScaleRecommendation(downscaleAction, keepWorkersAlive: true, reason: reason);
                }
                else if (this.controlQueueLengths.IsAllZeros())
                {
                    string reason;
                    bool keepWorkersAlive;
                        
                    if (noWorkItems)
                    {
                        reason = "Task hub idle";
                        keepWorkersAlive = false;
                    }
                    else
                    {
                        reason = "No control events; work items decreasing";
                        keepWorkersAlive = true;
                    }

                    ScaleAction downscaleAction = workerCount > 0 ? ScaleAction.RemoveWorker : ScaleAction.None;
                    return new ScaleRecommendation(downscaleAction, keepWorkersAlive, reason); 
                }
            }

            ScaleAction steadyStateAction = workerCount > 0 ? ScaleAction.None : ScaleAction.AddWorker;
            return new ScaleRecommendation(steadyStateAction, keepWorkersAlive: true, reason: "Load is steady");
        }

        /// <summary>
        /// Data structure containing the number of partitions and the aggregate
        /// number of messages across those control queue partitions.
        /// </summary>
        public struct ControlQueueData
        {
            /// <summary>
            /// Gets or sets the number of control queue partitions.
            /// </summary>
            public int PartitionCount { get; internal set; }

            /// <summary>
            /// Gets or sets the number of messages across all control queues.
            /// </summary>
            public int AggregateQueueLength { get; internal set; }
        }

        class QueueLengthHistory
        {
            const double TrendThreshold = 0.0;

            readonly int[] history;
            int next;
            int count;
            int lastValue;
            double? currentTrend;

            public QueueLengthHistory(int maxSize)
            {
                this.history = new int[maxSize];
            }

            public bool IsFull
            {
                get { return this.count == this.history.Length; }
            }

            public int Last => this.lastValue;

            public double CurrentTrend
            {
                get
                {
                    if (!this.IsFull)
                    {
                        return 0.0;
                    }

                    if (!this.currentTrend.HasValue)
                    {
                        int firstIndex = this.IsFull ? this.next : 0;
                        int first = this.history[firstIndex];
                        if (first == 0)
                        {
                            // discard trend information when the first item is a zero.
                            this.currentTrend = 0.0;
                        }
                        else
                        {
                            int sum = 0;
                            for (int i = 0; i < this.history.Length; i++)
                            {
                                sum += this.history[i];
                            }

                            double average = (double)sum / this.history.Length;
                            this.currentTrend = (average - first) / first;
                        }
                    }

                    return this.currentTrend.Value;
                }
            }

            public void Add(int value)
            {
                this.history[this.next++] = value;
                if (this.count < this.history.Length)
                {
                    this.count++;
                }

                if (this.next >= this.history.Length)
                {
                    this.next = 0;
                }

                this.lastValue = value;

                // invalidate any existing trend information
                this.currentTrend = null;
            }

            public bool IsTrendingUpwards()
            {
                return this.CurrentTrend > TrendThreshold;
            }

            public bool IsTrendingDownwards()
            {
                return this.CurrentTrend < -TrendThreshold;
            }

            public bool IsAllZeros()
            {
                return Array.TrueForAll(this.history, i => i == 0);
            }

            static void ThrowIfNegative(string paramName, double value)
            {
                if (value < 0.0)
                {
                    throw new ArgumentOutOfRangeException(paramName, value, $"{paramName} cannot be negative.");
                }
            }

            static void ThrowIfPositive(string paramName, double value)
            {
                if (value > 0.0)
                {
                    throw new ArgumentOutOfRangeException(paramName, value, $"{paramName} cannot be positive.");
                }
            }
        }
    }
}
