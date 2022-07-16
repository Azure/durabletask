namespace DurableTask.AzureStorage.Monitoring
{
    /// <summary>
    /// Possible scale actions for durable task hub.
    /// </summary>
    public enum ScaleAction
    {
        /// <summary>
        /// Do not add or remove workers.
        /// </summary>
        None = 0,

        /// <summary>
        /// Add workers to the current task hub.
        /// </summary>
        AddWorker,

        /// <summary>
        /// Remove workers from the current task hub.
        /// </summary>
        RemoveWorker
    }
}
