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
    using DurableTask.Core.Command;
    using DurableTask.Core.Common;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Middleware;

    public class TaskOrchestrationExecutor : OrchestrationExecutorBase
    {
        readonly TaskOrchestrationContext context;
        readonly TaskScheduler decisionScheduler;
        readonly OrchestrationRuntimeState orchestrationRuntimeState;
        readonly TaskOrchestration taskOrchestration;
        readonly bool skipCarryOverEvents;
        readonly Lazy<string> orchestrationStatus;
        Task<string>? result;

        public TaskOrchestrationExecutor(
            OrchestrationRuntimeState orchestrationRuntimeState,
            TaskOrchestration taskOrchestration,
            BehaviorOnContinueAsNew eventBehaviourForContinueAsNew)
            : base(orchestrationRuntimeState)
        {
            this.decisionScheduler = new SynchronousTaskScheduler();
            this.context = new TaskOrchestrationContext(orchestrationRuntimeState.OrchestrationInstance, this.decisionScheduler);
            this.orchestrationRuntimeState = orchestrationRuntimeState ?? throw new ArgumentNullException(nameof(orchestrationRuntimeState));
            this.taskOrchestration = taskOrchestration ?? throw new ArgumentNullException(nameof(taskOrchestration));
            this.skipCarryOverEvents = eventBehaviourForContinueAsNew == BehaviorOnContinueAsNew.Ignore;
            this.orchestrationStatus = new Lazy<string>(() => this.taskOrchestration.GetStatus());
        }

        public bool IsCompleted => this.result != null && (this.result.IsCompleted || this.result.IsFaulted);

        protected internal override DispatchMiddlewareContext CreateDispatchContext()
        {
            DispatchMiddlewareContext middlewareContext = base.CreateDispatchContext();
            middlewareContext.SetProperty(this.taskOrchestration);
            return middlewareContext;
        }

        public override Task<OrchestratorExecutionResult> ExecuteNewEventsAsync()
        {
            this.context.ClearPendingActions();
            return base.ExecuteNewEventsAsync();
        }

        protected override Task<OrchestratorExecutionResult> OnExecuteAsync(IEnumerable<HistoryEvent> pastEvents, IEnumerable<HistoryEvent> newEvents)
        {
            return Task.FromResult(this.ExecuteCore(pastEvents, newEvents));
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

                            ////this.context.IsReplaying = historyEvent.IsPlayed;
                            this.ProcessEvent(historyEvent);
                            ////historyEvent.IsPlayed = true;
                        }
                    }

                    // Replay the old history to rebuild the local state of the orchestration.
                    this.context.IsReplaying = true;
                    ProcessEvents(pastEvents);

                    // Play the newly arrived events to determine the next action to take.
                    this.context.IsReplaying = false;
                    ProcessEvents(newEvents);

                    // check if workflow is completed after this replay
                    if (!this.context.HasOpenTasks)
                    {
                        if (this.result != null && this.result.IsCompleted)
                        {
                            if (this.result.IsFaulted)
                            {
                                Exception? exception = this.result.Exception?.InnerExceptions.FirstOrDefault();
                                Debug.Assert(exception != null);

                                if (Utils.IsExecutionAborting(exception))
                                {
                                    // Let this exception propagate out to be handled by the dispatcher
                                    ExceptionDispatchInfo.Capture(exception).Throw();
                                }
                                
                                this.context.FailOrchestration(exception);
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
                    this.context.FailOrchestration(exception);
                }

                return new OrchestratorExecutionResult
                {
                    Actions = this.context.OrchestratorActions,
                    CustomStatus = this.orchestrationStatus.Value,
                };
            }
            finally
            {
                // NOTE: Legacy behavior for in-proc execution. Ideally we'd never modify orchestration runtime state here.
                this.orchestrationRuntimeState.Status = this.orchestrationStatus.Value;
                SynchronizationContext.SetSynchronizationContext(prevCtx);
                OrchestrationContext.IsOrchestratorThread = false;
            }
        }

        void ProcessEvent(HistoryEvent historyEvent)
        {
            switch (historyEvent.EventType)
            {
                case EventType.ExecutionStarted:
                    var executionStartedEvent = (ExecutionStartedEvent)historyEvent;
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
                    if (this.skipCarryOverEvents || !this.context.HasContinueAsNew)
                    {
                        var eventRaisedEvent = (EventRaisedEvent)historyEvent;
                        this.taskOrchestration.RaiseEvent(this.context, eventRaisedEvent.Name, eventRaisedEvent.Input);
                    }
                    else
                    {
                        this.context.AddEventToNextIteration(historyEvent);
                    }
                    break;
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