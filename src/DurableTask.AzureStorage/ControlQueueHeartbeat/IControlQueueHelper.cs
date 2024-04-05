using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;

namespace DurableTask.AzureStorage.ControlQueueHeartbeat
{
    /// <summary>Monitors control queue health for orchestrator's processing. Make sure to provide same <see cref="AzureStorageOrchestrationServiceSettings"/> setting for taskhubclient, taskhubworker and IControlQueueHealthMonitor.</summary>
    public interface IControlQueueHelper
    {
#nullable enable
        /// <summary>
        /// Sets up the TaskHub client and worker for control-queue heartbeat and detects if any of heartbeat orchestration running on each control-queue is not running.
        /// </summary>
        /// <param name="taskHubClient">TaskHubClient object.</param>
        /// <param name="taskHubWorker">TaskHubWorker object.</param>
        /// <param name="callBackHeartOrchAsync">Callback to run with each execution of the orchestrator of type <see cref="ControlQueueHeartbeatTaskOrchestrator"/>.</param>
        /// <param name="callBackControlQueueValidation">Callback to run with each time the detects fault of type <see cref="ControlQueueHeartbeatDetectionInfo"/>.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Task result.</returns>
        Task StartControlQueueHeartbeatMonitorAsync(
            TaskHubClient taskHubClient,
            TaskHubWorker taskHubWorker,
            Func<OrchestrationInstance, ControlQueueHeartbeatTaskInputContext, ControlQueueHeartbeatTaskContext, CancellationToken, Task> callBackHeartOrchAsync,
            Func<string, string?, bool, string, string, ControlQueueHeartbeatDetectionInfo, CancellationToken, Task> callBackControlQueueValidation,
            CancellationToken cancellationToken);
#nullable disable

        /// <summary>
        /// Adds orchestrator instances of type <see cref="ControlQueueHeartbeatTaskOrchestrator"/> for each control queue. 
        /// </summary>
        /// <param name="taskHubClient">TaskHubClient object.</param>
        /// <param name="cancellationToken">CancellationToken.</param>
        /// <param name="force">If true, creates new instances of orchestrator, otherwise creates only if there is no running instance of orchestrator with same instance id..</param>
        /// <returns>Task result.</returns>
        Task ScheduleControlQueueHeartbeatOrchestrationsAsync(TaskHubClient taskHubClient, CancellationToken cancellationToken, bool force = false);

        /// <summary>
        /// Adds orchestrator instance of type <see cref="ControlQueueHeartbeatTaskOrchestrator"/> to TaskHubWorkerObject.
        /// </summary>
        /// <param name="taskHubWorker">TaskHubWorker object.</param>
        /// <param name="callBackHeartOrchAsync">Callback to run with each execution of the orchestrator of type <see cref="ControlQueueHeartbeatTaskOrchestrator"/>.</param>
        void RegisterControlQueueHeartbeatOrchestration(
            TaskHubWorker taskHubWorker, 
            Func<OrchestrationInstance, ControlQueueHeartbeatTaskInputContext, ControlQueueHeartbeatTaskContext, CancellationToken, Task> callBackHeartOrchAsync);

        /// <summary>
        /// Gets instanceId which is targeted for mentioned control-queue names.
        /// </summary>
        /// <param name="controlQueueNumbers">Collection of controlQueueNumbers.</param>
        /// <param name="instanceIdPrefix">InstanceId prefix.</param>
        /// <returns>InstanceId for control-queue.</returns>
        string GetControlQueueInstanceId(HashSet<int>  controlQueueNumbers, string instanceIdPrefix = "");
    }
}
