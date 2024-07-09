namespace DurableTask.AzureStorage.ControlQueueHeartbeat
{
    internal enum ControlQueueHeartbeatTaskResult
    {
        /// <summary>
        /// Default value of ControlQueueHeartbeatTaskResult.
        /// </summary>
        Unknown,

        /// <summary>
        /// Orchestratoin succeeded.
        /// </summary>
        Succeeded,

        /// <summary>
        /// Invalid input.
        /// Happens when input is either null or not of type <see cref="ControlQueueHeartbeatTaskInputContext"/>.
        /// </summary>
        InvalidInput,

        /// <summary>
        /// Mismatch in heartbeat orchestration context and input to orchestration instance.
        /// This happens when the taskhubworker running with different partition count than that of orchestration instance queued with.
        /// Usually it happens when partition-count is changed for a taskhub. 
        /// </summary>
        InputContextMismatch
    }
}
