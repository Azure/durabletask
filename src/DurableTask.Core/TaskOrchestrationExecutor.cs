//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------
#nullable enable
namespace DurableTask.Core
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Runtime.ExceptionServices;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core.Common;
    using DurableTask.Core.Entities;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;

    /// <summary>
    /// Utility for executing task orchestrators.
    /// </summary>
    public class TaskOrchestrationExecutor
    {
        readonly TaskOrchestrationContext context;
        readonly TaskScheduler decisionScheduler;
        readonly OrchestrationRuntimeState orchestrationRuntimeState;
        readonly TaskOrchestration taskOrchestration;
        readonly bool skipCarryOverEvents;
        readonly IExceptionPropertiesProvider? exceptionPropertiesProvider;
        Task<string>? result;

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskOrchestrationExecutor"/> class.
        /// </summary>
        /// <param name="orchestrationRuntimeState"></param>
        /// <param name="taskOrchestration"></param>
        /// <param name="eventBehaviourForContinueAsNew"></param>
        /// <param name="entityParameters"></param>
        /// <param name="errorPropagationMode"></param>
        public TaskOrchestrationExecutor(
            OrchestrationRuntimeState orchestrationRuntimeState,
            TaskOrchestration taskOrchestration,
            BehaviorOnContinueAsNew eventBehaviourForContinueAsNew,
            TaskOrchestrationEntityParameters? entityParameters,
            ErrorPropagationMode errorPropagationMode = ErrorPropagationMode.SerializeExceptions)
            : this(orchestrationRuntimeState, taskOrchestration, eventBehaviourForContinueAsNew, entityParameters, errorPropagationMode, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskOrchestrationExecutor"/> class.
        /// This overload is needed only to avoid breaking changes because this is a public constructor.
        /// </summary>
        /// <param name="orchestrationRuntimeState"></param>
        /// <param name="taskOrchestration"></param>
        /// <param name="eventBehaviourForContinueAsNew"></param>
        /// <param name="errorPropagationMode"></param>
        public TaskOrchestrationExecutor(
            OrchestrationRuntimeState orchestrationRuntimeState,
            TaskOrchestration taskOrchestration,
            BehaviorOnContinueAsNew eventBehaviourForContinueAsNew,
            ErrorPropagationMode errorPropagationMode = ErrorPropagationMode.SerializeExceptions)
            : this(orchestrationRuntimeState, taskOrchestration, eventBehaviourForContinueAsNew, entityParameters: null, errorPropagationMode, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TaskOrchestrationExecutor"/> class.
        /// </summary>
        /// <param name="orchestrationRuntimeState"></param>
        /// <param name="taskOrchestration"></param>
        /// <param name="eventBehaviourForContinueAsNew"></param>
        /// <param name="entityParameters"></param>
        /// <param name="errorPropagationMode"></param>
        /// <param name="exceptionPropertiesProvider"></param>
        public TaskOrchestrationExecutor(
            OrchestrationRuntimeState orchestrationRuntimeState,
            TaskOrchestration taskOrchestration,
            BehaviorOnContinueAsNew eventBehaviourForContinueAsNew,
            TaskOrchestrationEntityParameters? entityParameters,
            ErrorPropagationMode errorPropagationMode,
            IExceptionPropertiesProvider? exceptionPropertiesProvider)
        {
            this.decisionScheduler = new SynchronousTaskScheduler();
            this.context = new TaskOrchestrationContext(
                orchestrationRuntimeState.OrchestrationInstance,
                this.decisionScheduler,
                entityParameters,
                errorPropagationMode,
                exceptionPropertiesProvider);
            this.orchestrationRuntimeState = orchestrationRuntimeState;
            this.taskOrchestration = taskOrchestration;
            this.skipCarryOverEvents = eventBehaviourForContinueAsNew == BehaviorOnContinueAsNew.Ignore;
            this.exceptionPropertiesProvider = exceptionPropertiesProvider;
        }

        /// <summary>
        /// Whether or not the orchestration has completed.
        /// </summary>
        public bool IsCompleted => this.result != null && (this.result.IsCompleted || this.result.IsFaulted);

        /// <summary>
        /// Executes an orchestration from the beginning.
        /// </summary>
        /// <returns>
        /// The result of the orchestration execution, including any actions scheduled by the orchestrator.
        /// </returns>
        public OrchestratorExecutionResult Execute()
        {
            return this.ExecuteCore(
                pastEvents: this.orchestrationRuntimeState.PastEvents,
                newEvents: this.orchestrationRuntimeState.NewEvents);
        }

        /// <summary>
        /// Resumes an orchestration
        /// </summary>
        /// <returns>
        /// The result of the orchestration execution, including any actions scheduled by the orchestrator.
        /// </returns>
        public OrchestratorExecutionResult ExecuteNewEvents()
        {
            this.context.ClearPendingActions();
            return this.ExecuteCore(
                pastEvents: Enumerable.Empty<HistoryEvent>(),
                newEvents: this.orchestrationRuntimeState.NewEvents);
        }

        /// <summary>
        /// Releases the orchestrator continuations that this executor abandoned while waiting on
        /// activities, sub-orchestrations, or timers.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Call this once the executor is guaranteed never to run again, i.e. when the orchestration
        /// session ends. It must not be called between episodes of an extended session, because an open
        /// task still needs to be able to receive its result on a later episode.
        /// </para>
        /// <para>
        /// Skipping this call is not a correctness problem, but it leaks memory whenever a debugger is
        /// attached: the CLR keeps every awaited task in a process-wide dictionary until that task
        /// completes, so abandoned orchestrator awaits permanently root the orchestration object graph.
        /// See https://github.com/Azure/azure-functions-durable-extension/issues/340.
        /// </para>
        /// <para>
        /// This is intentionally internal rather than an <see cref="IDisposable"/> implementation.
        /// Executor lifetime is owned by <see cref="TaskOrchestrationDispatcher"/>, which is the only
        /// component that knows when an executor will never run again, so exposing this externally would
        /// add public surface that no caller outside this assembly can use correctly.
        /// </para>
        /// </remarks>
        internal void Release()
        {
            SynchronizationContext prevCtx = SynchronizationContext.Current;
            bool prevIsOrchestratorThread = OrchestrationContext.IsOrchestratorThread;

            try
            {
                // Cancelling the open tasks resumes orchestrator code, so give it the same ambient
                // environment it sees during a normal episode.
                SynchronizationContext.SetSynchronizationContext(
                    new TaskOrchestrationSynchronizationContext(this.decisionScheduler));
                OrchestrationContext.IsOrchestratorThread = true;

                this.context.ReleaseOpenTasks();
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(prevCtx);
                OrchestrationContext.IsOrchestratorThread = prevIsOrchestratorThread;

                // Unwinding may have faulted the orchestrator's top-level task. Nothing observes it at this
                // point, so observe it here to keep it from surfacing as an unobserved task exception.
                if (this.result?.IsFaulted == true)
                {
                    _ = this.result.Exception;
                }
            }
        }

        OrchestratorExecutionResult ExecuteCore(IEnumerable<HistoryEvent> pastEvents, IEnumerable<HistoryEvent> newEvents)
        {
            SynchronizationContext prevCtx = SynchronizationContext.Current;

            try
            {
                SynchronizationContext syncCtx = new TaskOrchestrationSynchronizationContext(this.decisionScheduler);
                SynchronizationContext.SetSynchronizationContext(syncCtx);
                OrchestrationContext.IsOrchestratorThread = true;

                try
                {
                    void ProcessEvents(IEnumerable<HistoryEvent> events)
                    {
                        foreach (HistoryEvent historyEvent in events)
                        {
                            if (historyEvent.EventType == EventType.OrchestratorStarted)
                            {
                                var decisionStartedEvent = (OrchestratorStartedEvent)historyEvent;
                                this.context.CurrentUtcDateTime = decisionStartedEvent.Timestamp;
                                continue;
                            }

                            this.ProcessEvent(historyEvent);
                            historyEvent.IsPlayed = true;
                        }
                    }

                    // Replay the old history to rebuild the local state of the orchestration.
                    // TODO: Log a verbose message indicating that the replay has started (include event count?)
                    this.context.IsReplaying = true;
                    ProcessEvents(pastEvents);

                    // Play the newly arrived events to determine the next action to take.
                    // TODO: Log a verbose message indicating that new events are being processed (include event count?)
                    this.context.IsReplaying = false;
                    ProcessEvents(newEvents);

                    // check if workflow is completed after this replay
                    // TODO: Create a setting that allows orchestrations to complete when the orchestrator
                    //       function completes, even if there are open tasks.
                    if (!this.context.HasOpenTasks)
                    {
                        if (this.result!.IsCompleted)
                        {
                            if (this.result.IsFaulted)
                            {
                                Exception? exception = this.result.Exception?.InnerExceptions.FirstOrDefault();
                                Debug.Assert(exception != null);

                                if (Utils.IsExecutionAborting(exception!))
                                {
                                    // Let this exception propagate out to be handled by the dispatcher
                                    ExceptionDispatchInfo.Capture(exception).Throw();
                                }
                                
                                this.context.FailOrchestration(exception, this.orchestrationRuntimeState);
                            }
                            else
                            {
                                this.context.CompleteOrchestration(this.result.Result);
                            }
                        }

                        // TODO: It is an error if result is not completed when all OpenTasks are done.
                        // Throw an exception in that case.
                    }
                }
                catch (NonDeterministicOrchestrationException exception)
                {
                    this.context.FailOrchestration(exception, this.orchestrationRuntimeState);
                }

                return new OrchestratorExecutionResult
                {
                    Actions = this.context.OrchestratorActions,
                    CustomStatus = this.taskOrchestration.GetStatus(),
                };
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(prevCtx);
                OrchestrationContext.IsOrchestratorThread = false;
            }
        }

        void ProcessEvent(HistoryEvent historyEvent)
        {
            bool overrideSuspension = historyEvent.EventType == EventType.ExecutionResumed || historyEvent.EventType == EventType.ExecutionTerminated;
            if (this.context.IsSuspended && !overrideSuspension)
            {
                this.context.HandleEventWhileSuspended(historyEvent);
            }
            else
            {
                switch (historyEvent.EventType)
                {
                    case EventType.ExecutionStarted:
                        var executionStartedEvent = (ExecutionStartedEvent)historyEvent;
                        this.context.Version = executionStartedEvent.Version;
                        this.result = this.taskOrchestration.Execute(this.context, executionStartedEvent.Input);
                        break;
                    case EventType.ExecutionTerminated:
                        this.context.HandleExecutionTerminatedEvent((ExecutionTerminatedEvent)historyEvent);
                        break;
                    case EventType.TaskScheduled:
                        this.context.HandleTaskScheduledEvent((TaskScheduledEvent)historyEvent);
                        break;
                    case EventType.TaskCompleted:
                        this.context.HandleTaskCompletedEvent((TaskCompletedEvent)historyEvent);
                        break;
                    case EventType.TaskFailed:
                        this.context.HandleTaskFailedEvent((TaskFailedEvent)historyEvent);
                        break;
                    case EventType.SubOrchestrationInstanceCreated:
                        this.context.HandleSubOrchestrationCreatedEvent((SubOrchestrationInstanceCreatedEvent)historyEvent);
                        break;
                    case EventType.SubOrchestrationInstanceCompleted:
                        this.context.HandleSubOrchestrationInstanceCompletedEvent(
                            (SubOrchestrationInstanceCompletedEvent)historyEvent);
                        break;
                    case EventType.SubOrchestrationInstanceFailed:
                        this.context.HandleSubOrchestrationInstanceFailedEvent((SubOrchestrationInstanceFailedEvent)historyEvent);
                        break;
                    case EventType.TimerCreated:
                        this.context.HandleTimerCreatedEvent((TimerCreatedEvent)historyEvent);
                        break;
                    case EventType.TimerFired:
                        this.context.HandleTimerFiredEvent((TimerFiredEvent)historyEvent);
                        break;
                    case EventType.EventSent:
                        this.context.HandleEventSentEvent((EventSentEvent)historyEvent);
                        break;
                    case EventType.EventRaised:
                        this.context.HandleEventRaisedEvent((EventRaisedEvent)historyEvent, this.skipCarryOverEvents, this.taskOrchestration);
                        break;
                    case EventType.ExecutionSuspended:
                        this.context.HandleExecutionSuspendedEvent((ExecutionSuspendedEvent)historyEvent);
                        break;
                    case EventType.ExecutionResumed:
                        this.context.HandleExecutionResumedEvent((ExecutionResumedEvent)historyEvent, ProcessEvent);
                        break;
                }
            }
        }

        class TaskOrchestrationSynchronizationContext : SynchronizationContext
        {
            readonly TaskScheduler scheduler;

            public TaskOrchestrationSynchronizationContext(TaskScheduler scheduler)
            {
                this.scheduler = scheduler;
            }

            public override void Post(SendOrPostCallback sendOrPostCallback, object state)
            {
                Task.Factory.StartNew(() => sendOrPostCallback(state),
                    CancellationToken.None,
                    TaskCreationOptions.None,
                    this.scheduler);
            }

            public override void Send(SendOrPostCallback sendOrPostCallback, object state)
            {
                var t = new Task(() => sendOrPostCallback(state));
                t.RunSynchronously(this.scheduler);
                t.Wait();
            }
        }
    }
}