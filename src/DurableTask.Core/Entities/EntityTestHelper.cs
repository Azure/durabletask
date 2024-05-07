using DurableTask.Core.History;
using DurableTask.Core.Logging;
using DurableTask.Core.Middleware;
using Microsoft.Extensions.Logging;

namespace DurableTask.Core.Entities
{
    /// <summary>
    /// Class to help test TaskEntityDispatcher.
    /// </summary>
    public class EntityTestHelper
    {
        IOrchestrationService orchestrationService;
        TaskEntityDispatcher dispatcher;

        /// <summary>
        /// Constructor for EntityTestHelper
        /// </summary>
        /// <param name="service">The <see cref="IOrchestrationService"/> for initialize TaskEntityDispatcher instance.</param>
        public EntityTestHelper(IOrchestrationService service)
        {
            this.orchestrationService = service;
            this.dispatcher = CreateTestEntityDispatcherInstance();
        }

        /// <summary>
        /// Create an instance of the TaskEntityDispatcher class for testing.
        /// </summary>
        /// <returns>Instance created for testing.</returns>
        public TaskEntityDispatcher CreateTestEntityDispatcherInstance()
        {
            ILoggerFactory loggerFactory = null;
            var entityManager = new NameVersionObjectManager<TaskEntity>();
            var entityMiddleware = new DispatchMiddlewarePipeline();
            var logger = new LogHelper(loggerFactory?.CreateLogger("DurableTask.Core"));

            return  new TaskEntityDispatcher
            (
                this.orchestrationService,
                entityManager,
                entityMiddleware,
                logger,
                ErrorPropagationMode.SerializeExceptions);
        }

        /// <summary>
        /// Method to test orchestration settings when entities schedule it.
        /// </summary>
        /// <returns> HistoryEvent contains the scheduled message.</returns>
        public HistoryEvent TestScheduleOrchestrationTags()
        {
            return this.dispatcher.TestScheduleSendEvent();
        }
    }
}
