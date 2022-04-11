
namespace DurableTask.Core.Query
{
    using System.Threading.Tasks;
    using System.Threading;

    /// <summary>
    /// Interface to allow query multi-instance status with filter.
    /// </summary>
    public interface IOrchestrationServiceQueryClient
    {
        /// <summary>
        /// Gets the status of all orchestration instances with paging that match the specified conditions.
        /// </summary>
        /// <param name="condition">Return orchestration instances that match the specified conditions.</param>
        /// <param name="cancellationToken">Cancellation token that can be used to cancel the status query operation.</param>
        /// <returns>Returns each page of orchestration status for all instances and continuation token of next page.</returns>
        Task GetOrchestrationStateWithFiltersAsync(OrchestrationStatusQueryCondition condition, CancellationToken cancellationToken);
    }
}
