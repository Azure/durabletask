namespace DurableTask.AzureStorage.ControlQueueHeartbeat
{
    /// <summary>
    /// Input for ControlQueueHeartbeat orchestration instance.
    /// </summary>
    public class ControlQueueHeartbeatTaskInputContext : ControlQueueHeartbeatTaskContext
    {
        /// <summary>
        /// Control-queue name.
        /// </summary>
        public string ControlQueueName { get; private set; }

        /// <summary>
        /// ControlQueueHeartbeatTaskInputContext constructor.
        /// </summary>
        /// <param name="controlQueueName">Name of control queue.</param>
        /// <param name="taskhubName">Name of taskhub.</param>
        /// <param name="partitionCount">Name of partitionCount.</param>
        public ControlQueueHeartbeatTaskInputContext(string controlQueueName, string taskhubName, int partitionCount)
            : base(taskhubName, partitionCount)
        {
            this.ControlQueueName = controlQueueName;
        }

        /// <summary> Returns a string that represents the OrchestrationInstance.</summary>
        /// <returns>A string that represents the current object.</returns>
        public override string ToString()
        {
            return $"[ControlQueueName={ControlQueueName}, TaskHubName={TaskHubName}, PartitionCount = {this.PartitionCount}]";
        }
    }
}
