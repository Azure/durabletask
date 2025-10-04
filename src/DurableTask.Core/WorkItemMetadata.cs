namespace DurableTask.Core
{
    /// <summary>
    /// A class representing metadata information about a work item.
    /// </summary>
    public class WorkItemMetadata
    {
        /// <summary>
        /// Gets or sets whether or not the execution of the work item is within an extended session. 
        /// </summary>
        public bool IsExtendedSession { get; set; }

        /// <summary>
        /// Gets or sets whether or not to include past events in the orchestration history when executing the work item via middleware.
        /// This assumes that the middleware is able to handle extended sessions and does not require history for replays.
        /// </summary>
        public bool IncludePastEvents { get; set; }
    }
}
