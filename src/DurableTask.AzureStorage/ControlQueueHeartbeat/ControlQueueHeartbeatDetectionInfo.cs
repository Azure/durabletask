namespace DurableTask.AzureStorage.ControlQueueHeartbeat
{
    /// <summary>
    /// Control-queue heartbeat detection information.
    /// </summary>
    public enum ControlQueueHeartbeatDetectionInfo
    {
        /// <summary>
        /// Default value.
        /// </summary>
        Unknown,

        /// <summary>
        /// The orchestration instance for control-queue is stuck.
        /// </summary>
        OrchestrationInstanceStuck,

        /// <summary>
        /// Expected orchestration instance for control-queue timed out.
        /// </summary>
        OrchestrationInstanceFetchTimedOut,

        /// <summary>
        /// Expected orchestration instance for control-queue not found.
        /// </summary>
        OrchestrationInstanceNotFound,

        /// <summary>
        /// Control-queue owner information fetch failed.
        /// </summary>
        ControlQueueOwnerFetchFailed
    }
}
