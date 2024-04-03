namespace DurableTask.AzureStorage.ControlQueueHeartbeat
{
    /// <summary>
    /// Context for ControlQueueHeartbeat task orchestrator.
    /// </summary>
    public class ControlQueueHeartbeatTaskContext
    {
        /// <summary> 
        /// Name of taskhub.    
        /// </summary>
        public string TaskHubName { get; private set; }

        /// <summary> 
        /// Number of partitions.
        /// </summary>
        public int PartitionCount { get; private set; }

        /// <summary> 
        /// ControlQueueHeartbeatTaskContext constructor.
        /// </summary>
        /// <param name="taskhubName">Name of taskhub.</param>
        /// <param name="partitionCount">Number of partitions.</param>
        public ControlQueueHeartbeatTaskContext(string taskhubName, int partitionCount)
        {
            this.TaskHubName = taskhubName;
            this.PartitionCount = partitionCount;
        }

        /// <summary>
        /// Returns a string that represents the OrchestrationInstance.
        /// </summary>
        /// <returns>A string that represents the current object.</returns>
        public override string ToString()
        {
            return $"[TaskHubName={TaskHubName}, PartitionCount = {this.PartitionCount}]";
        }
    }
}
