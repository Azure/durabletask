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
    internal class ControlQueueHeartbeatTaskOrchestratorV1 : TaskOrchestration<string, ControlQueueHeartbeatTaskInputContext>
    {
        public const string OrchestrationName = "ControlQueueHeartbeatTaskOrchestrator";

        public const string OrchestrationVersion = "V1";

        private ControlQueueHeartbeatTaskContext controlQueueHeartbeatTaskContextInit;

        private TimeSpan controlQueueHearbeatOrchestrationInterval;

        private Func<OrchestrationInstance, ControlQueueHeartbeatTaskInputContext, ControlQueueHeartbeatTaskContext, CancellationToken, Task> callBack;

        private SemaphoreSlim semaphoreSlim;

        /// <summary>
        /// ControlQueueHeartbeatTaskOrchestratorV1 constructor.
        /// </summary>
        /// <param name="controlQueueHeartbeatTaskContext">ControlQueueHeartbeatTaskContext object, informs about configuration of taskhubworker orchestrator is running in.</param>
        /// <param name="controlQueueHearbeatOrchestrationInterval">Interval between two heartbeats.</param>
        /// <param name="callBack">
        ///     A callback to allow user process/emit custom metrics for heartbeat execution.
        /// </param>
        /// <exception cref="ArgumentNullException">Throws if provided ControlQueueHeartbeatTaskContext object is null.</exception>
        internal ControlQueueHeartbeatTaskOrchestratorV1(
            ControlQueueHeartbeatTaskContext controlQueueHeartbeatTaskContext,
            TimeSpan controlQueueHearbeatOrchestrationInterval,
            Func<OrchestrationInstance, ControlQueueHeartbeatTaskInputContext, ControlQueueHeartbeatTaskContext, CancellationToken, Task> callBack)
        {
            this.controlQueueHeartbeatTaskContextInit = controlQueueHeartbeatTaskContext ?? throw new ArgumentNullException(nameof(controlQueueHeartbeatTaskContext));
            this.controlQueueHearbeatOrchestrationInterval = controlQueueHearbeatOrchestrationInterval;
            this.callBack = callBack;

            // At worst case, allowing max of 2 heartbeat of a control-queue to run callbacks. 
            this.semaphoreSlim = new SemaphoreSlim(2 * controlQueueHeartbeatTaskContext.PartitionCount);
        }

        public override async Task<string> RunTask(OrchestrationContext context, ControlQueueHeartbeatTaskInputContext controlQueueHeartbeatTaskContextInput)
        {
            // Stopwatch to calculate time to complete orchestrator.
            Stopwatch stopwatchOrch = Stopwatch.StartNew();

            // Checks for input being null and complete gracefully.
            if (controlQueueHeartbeatTaskContextInput == null)
            {
                // [Logs] Add log for failure of the orchestrator.
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
                    var gotSemaphore = await semaphoreSlim.WaitAsync(controlQueueHearbeatOrchestrationInterval);
                    Stopwatch stopwatch = Stopwatch.StartNew();

                    try
                    {
                        if (gotSemaphore)
                        {
                            var cancellationTokenSrc = new CancellationTokenSource();
                            var delayTask = Task.Delay(controlQueueHearbeatOrchestrationInterval);
                            var callBackTask = this.callBack(context.OrchestrationInstance, controlQueueHeartbeatTaskContextInput, controlQueueHeartbeatTaskContextInit, cancellationTokenSrc.Token);

                            // Do not allow callBackTask to run forever.
                            await Task.WhenAll(callBackTask, delayTask);

                            if (!callBackTask.IsCompleted)
                            {
                                // [Logs] Add log for long running callback.
                                FileWriter.WriteLogControlQueueOrch($"ControlQueueHeartbeatTaskOrchestratorCallbackTerminated " +
                                    $"OrchestrationInstance:{context.OrchestrationInstance} " +
                                    $"controlQueueHeartbeatTaskContextInit:{controlQueueHeartbeatTaskContextInit}, " +
                                    $"controlQueueHeartbeatTaskContextInput: {controlQueueHeartbeatTaskContextInput}" +
                                    $"duration: {stopwatch.ElapsedMilliseconds}" +
                                    $"message: callback is taking too long to cmplete.");
                            }

                            cancellationTokenSrc.Cancel();
                        }
                        else
                        {
                            // [Logs] Add log for too many callbacks running. Share the semaphore-count for #callBacks allowed, and wait time for semaphore; and ask to reduce the run-time for callback.
                            FileWriter.WriteLogControlQueueOrch($"ControlQueueHeartbeatTaskOrchestratorTooManyActiveCallback " +
                                $"OrchestrationInstance:{context.OrchestrationInstance} " +
                                $"controlQueueHeartbeatTaskContextInit:{controlQueueHeartbeatTaskContextInit}, " +
                                $"controlQueueHeartbeatTaskContextInput: {controlQueueHeartbeatTaskContextInput}" +
                                $"duration: {stopwatch.ElapsedMilliseconds}" +
                                $"message: too many active callbacks, skipping runnning this instance of callback.");
                        }
                    }
                    // Not throwing anything beyond this.
                    catch (Exception ex)
                    {
                        // [Logs] Add exception details for callback failure.
                        FileWriter.WriteLogControlQueueOrch($"ControlQueueHeartbeatTaskOrchestratorCallbackFailure " +
                            $"OrchestrationInstance:{context.OrchestrationInstance} " +
                            $"controlQueueHeartbeatTaskContextInit:{controlQueueHeartbeatTaskContextInit}, " +
                            $"controlQueueHeartbeatTaskContextInput: {controlQueueHeartbeatTaskContextInput}" +
                            $"exception: {ex.ToString()}" +
                            $"duration: {stopwatch.ElapsedMilliseconds}" +
                            $"message: the task was failed.");
                    }
                    // ensuring semaphore is released.
                    finally
                    {
                        if (gotSemaphore)
                        {
                            semaphoreSlim.Release();
                        }
                    }
                });

                if (!isQueued)
                {
                    FileWriter.WriteLogControlQueueOrch($"ControlQueueHeartbeatTaskOrchestratorV1 Stopping OrchestrationInstance:{context.OrchestrationInstance} controlQueueHeartbeatTaskContextInit:{controlQueueHeartbeatTaskContextInit}, controlQueueHeartbeatTaskContextInput: {controlQueueHeartbeatTaskContextInput}");

                    // [Logs] Add log for a heartbeat message from current instance. 
                    FileWriter.WriteLogControlQueueOrch($"ControlQueueHeartbeatTaskOrchestratorCallbackNotQueued " +
                        $"OrchestrationInstance:{context.OrchestrationInstance} " +
                        $"controlQueueHeartbeatTaskContextInit:{controlQueueHeartbeatTaskContextInit}, " +
                        $"controlQueueHeartbeatTaskContextInput: {controlQueueHeartbeatTaskContextInput}" +
                        $"duration: {stopwatchOrch.ElapsedMilliseconds}" +
                        $"message: Callback for orchestrator could not be queued.");
                }
            }

            // [Logs] Add log for a heartbeat message from current instance. 
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
