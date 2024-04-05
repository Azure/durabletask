using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;

namespace DurableTask.AzureStorage.ControlQueueHeartbeat
{
    /// <summary>
    /// Control-queue heartbeat orchestrator.
    /// This is supposed to be initialized with ControlQueueHeartbeatTaskContext informing orchestrator about configuration of taskhubworker and heartbeat interval.
    /// </summary>
    internal class ControlQueueHeartbeatTaskOrchestrator : TaskOrchestration<string, ControlQueueHeartbeatTaskInputContext>
    {
        public const string OrchestrationName = "ControlQueueHeartbeatTaskOrchestrator";

        public const string OrchestrationVersion = "V1";

        private ControlQueueHeartbeatTaskContext controlQueueHeartbeatTaskContextInit;

        private TimeSpan controlQueueHearbeatOrchestrationInterval;

        private Func<OrchestrationInstance, ControlQueueHeartbeatTaskInputContext, ControlQueueHeartbeatTaskContext, CancellationToken, Task> callBack;

        private CancellationTokenSource cancellationTokenSrc;

        /// <summary>
        /// ControlQueueHeartbeatTaskOrchestrator constructor.
        /// </summary>
        /// <param name="controlQueueHeartbeatTaskContext">ControlQueueHeartbeatTaskContext object, informs about configuration of taskhubworker orchestrator is running in.</param>
        /// <param name="controlQueueHearbeatOrchestrationInterval">Interval between two heartbeats.</param>
        /// <param name="callBack">
        ///     A callback to allow user process/emit custom metrics for heartbeat execution.
        /// </param>
        /// <exception cref="ArgumentNullException">Throws if provided ControlQueueHeartbeatTaskContext object is null.</exception>
        internal ControlQueueHeartbeatTaskOrchestrator(
            ControlQueueHeartbeatTaskContext controlQueueHeartbeatTaskContext,
            TimeSpan controlQueueHearbeatOrchestrationInterval,
            Func<OrchestrationInstance, ControlQueueHeartbeatTaskInputContext, ControlQueueHeartbeatTaskContext, CancellationToken, Task> callBack)
        {
            this.controlQueueHeartbeatTaskContextInit = controlQueueHeartbeatTaskContext ?? throw new ArgumentNullException(nameof(controlQueueHeartbeatTaskContext));
            this.controlQueueHearbeatOrchestrationInterval = controlQueueHearbeatOrchestrationInterval;
            this.callBack = callBack;

            this.cancellationTokenSrc = new CancellationTokenSource();
        }

        public override async Task<string> RunTask(OrchestrationContext context, ControlQueueHeartbeatTaskInputContext controlQueueHeartbeatTaskContextInput)
        {
            // Stopwatch to calculate time to complete orchestrator.
            Stopwatch stopwatchOrch = Stopwatch.StartNew();

            // Checks for input being null and complete gracefully.
            if (controlQueueHeartbeatTaskContextInput == null)
            {
                // [Logs] Add log for failure of the orchestrator.
                // Structured logging: ControlQueueHeartbeatTaskOrchestratorFailed
                // -> orchestrationInstance: context.OrchestrationInstance.ToString()
                // -> initialControlQueueHeartbeatTaskContext: controlQueueHeartbeatTaskContextInit.ToString()
                // -> inputControlQueueHeartbeatTaskContext: null
                // -> duration: stopwatchOrch.ElapsedMilliseconds
                // -> message : input context orchestration is null.
                FileWriter.WriteLogControlQueueOrch($"ControlQueueHeartbeatTaskOrchestratorFailed." +
                    $"OrchestrationInstance:{context.OrchestrationInstance} " +
                    $"controlQueueHeartbeatTaskContextInit:{controlQueueHeartbeatTaskContextInit}, " +
                    $"duration: {stopwatchOrch.ElapsedMilliseconds}" +
                    $"message: controlQueueHeartbeatTaskContextInput is null. Completing the orchestration.");

                return "Failed";
            }

            var isOrchestratorRunningInCorrectContext = controlQueueHeartbeatTaskContextInput.PartitionCount == controlQueueHeartbeatTaskContextInit.PartitionCount
                && controlQueueHeartbeatTaskContextInit.TaskHubName.Equals(controlQueueHeartbeatTaskContextInput.TaskHubName);

            // Checks if the context of orchestrator instance and orchestrator mismatch and complete gracefully.
            if (!isOrchestratorRunningInCorrectContext)
            {
                // [Logs] Add log for mistmatch in context.
                // Structured logging: ControlQueueHeartbeatTaskOrchestratorFailed
                // -> orchestrationInstance: context.OrchestrationInstance.ToString()
                // -> initialControlQueueHeartbeatTaskContext: controlQueueHeartbeatTaskContextInit.ToString()
                // -> inputControlQueueHeartbeatTaskContext: controlQueueHeartbeatTaskContextInit.ToString()
                // -> duration: stopwatchOrch.ElapsedMilliseconds
                // -> message : Input and initial context for orchestration .
                FileWriter.WriteLogControlQueueOrch($"ControlQueueHeartbeatTaskOrchestratorContextMismatch" +
                    $"OrchestrationInstance:{context.OrchestrationInstance} " +
                    $"controlQueueHeartbeatTaskContextInit:{controlQueueHeartbeatTaskContextInit}, " +
                    $"controlQueueHeartbeatTaskContextInput: {controlQueueHeartbeatTaskContextInput}" +
                    $"duration: {stopwatchOrch.ElapsedMilliseconds}" +
                    $"message: the partition count and taskhub information are not matching.");

                return "Failed";
            }

            // Waiting for heartbeat orchestration interval.
            await context.CreateTimer(context.CurrentUtcDateTime.Add(controlQueueHearbeatOrchestrationInterval), true);

            // Ensuring this section doesn't run again. 
            // This queues the user provided callback without waiting for it to finish.
            // This is to keep heartbeat orchestrator thin and fast.
            if (!context.IsReplaying)
            {
                // No wait to complete provided delegate. The current orchestrator need to be very thin and quick to run. 
                bool isQueued = ThreadPool.QueueUserWorkItem(async (_) =>
                {
                    await this.callBack(context.OrchestrationInstance, controlQueueHeartbeatTaskContextInput, controlQueueHeartbeatTaskContextInit, this.cancellationTokenSrc.Token);
                });

                if (!isQueued)
                {
                    // [Logs] Add log for a heartbeat message from current instance. 
                    // Structured logging: ControlQueueHeartbeatTaskOrchestratorCallbackNotQueued
                    // -> orchestrationInstance: context.OrchestrationInstance.ToString()
                    // -> initialControlQueueHeartbeatTaskContext: controlQueueHeartbeatTaskContextInit.ToString()
                    // -> inputControlQueueHeartbeatTaskContext: controlQueueHeartbeatTaskContextInit.ToString()
                    // -> duration: stopwatchOrch.ElapsedMilliseconds
                    FileWriter.WriteLogControlQueueOrch($"ControlQueueHeartbeatTaskOrchestratorCallbackNotQueued " +
                        $"OrchestrationInstance:{context.OrchestrationInstance} " +
                        $"controlQueueHeartbeatTaskContextInit:{controlQueueHeartbeatTaskContextInit}, " +
                        $"controlQueueHeartbeatTaskContextInput: {controlQueueHeartbeatTaskContextInput}" +
                        $"duration: {stopwatchOrch.ElapsedMilliseconds}" +
                        $"message: Callback for orchestrator could not be queued.");
                }
            }

            // [Logs] Add log for a heartbeat message from current instance. 
            // Structured logging: ControlQueueHeartbeatTaskOrchestrator
            // -> orchestrationInstance: context.OrchestrationInstance.ToString()
            // -> initialControlQueueHeartbeatTaskContext: controlQueueHeartbeatTaskContextInit.ToString()
            // -> inputControlQueueHeartbeatTaskContext: controlQueueHeartbeatTaskContextInit.ToString()
            // -> duration: stopwatchOrch.ElapsedMilliseconds
            FileWriter.WriteLogControlQueueOrch($"ControlQueueHeartbeatTaskOrchestrator " +
                $"OrchestrationInstance:{context.OrchestrationInstance} " +
                $"controlQueueHeartbeatTaskContextInit:{controlQueueHeartbeatTaskContextInit}, " +
                $"controlQueueHeartbeatTaskContextInput: {controlQueueHeartbeatTaskContextInput}" +
                $"duration: {stopwatchOrch.ElapsedMilliseconds}" +
                $"message: Sending signal for control-queue heartbeat.");

            context.ContinueAsNew(controlQueueHeartbeatTaskContextInput);

            return "Succeeded";
        }
    }
}
