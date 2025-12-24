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
        /// Gets or sets whether or not to include instance state when executing the work item via middleware.
        /// When false, this assumes that the middleware is able to handle extended sessions and has already cached
        /// the instance state from a previous execution, so it does not need to be included again.
        /// </summary>
        public bool IncludeState { get; set; }
    }
}
