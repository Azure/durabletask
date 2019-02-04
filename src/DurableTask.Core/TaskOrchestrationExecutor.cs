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

namespace DurableTask.Core
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core.Command;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;

    internal class TaskOrchestrationExecutor
    {
        readonly TaskOrchestrationContext context;
        readonly TaskScheduler decisionScheduler;
        readonly OrchestrationRuntimeState orchestrationRuntimeState;
        readonly TaskOrchestration taskOrchestration;
        readonly bool skipCarryOverEvents;
        Task<string> result;

        public TaskOrchestrationExecutor(OrchestrationRuntimeState orchestrationRuntimeState,
            TaskOrchestration taskOrchestration, BehaviorOnContinueAsNew eventBehaviourForContinueAsNew)
        {
            this.decisionScheduler = new SynchronousTaskScheduler();
            this.context = new TaskOrchestrationContext(orchestrationRuntimeState.OrchestrationInstance, this.decisionScheduler);
            this.orchestrationRuntimeState = orchestrationRuntimeState;
            this.taskOrchestration = taskOrchestration;
            this.skipCarryOverEvents = eventBehaviourForContinueAsNew == BehaviorOnContinueAsNew.Ignore;
        }

        public bool IsCompleted => this.result != null && (this.result.IsCompleted || this.result.IsFaulted);

        public IEnumerable<OrchestratorAction> Execute()
        {
            return ExecuteCore(this.orchestrationRuntimeState.Events);
        }

        public IEnumerable<OrchestratorAction> ExecuteNewEvents()
        {
            this.context.ClearPendingActions();
            return ExecuteCore(this.orchestrationRuntimeState.NewEvents);
        }

        IEnumerable<OrchestratorAction> ExecuteCore(IEnumerable<HistoryEvent> eventHistory)
        {
            SynchronizationContext prevCtx = SynchronizationContext.Current;

            try
            {
                SynchronizationContext syncCtx = new TaskOrchestrationSynchronizationContext(this.decisionScheduler);
                SynchronizationContext.SetSynchronizationContext(syncCtx);
                OrchestrationContext.IsOrchestratorThread = true;

                try
                {
                    foreach (HistoryEvent historyEvent in eventHistory)
                    {
                        if (historyEvent.EventType == EventType.OrchestratorStarted)
                        {
                            var decisionStartedEvent = (OrchestratorStartedEvent)historyEvent;
                            this.context.CurrentUtcDateTime = decisionStartedEvent.Timestamp;
                            continue;
                        }

                        this.context.IsReplaying = historyEvent.IsPlayed;
                        ProcessEvent(historyEvent);
                        historyEvent.IsPlayed = true;
                    }

                    // check if workflow is completed after this replay
                    if (!this.context.HasOpenTasks)
                    {
                        if (this.result.IsCompleted)
                        {
                            if (this.result.IsFaulted)
                            {
                                Debug.Assert(this.result.Exception != null);

                                this.context.FailOrchestration(this.result.Exception.InnerExceptions.FirstOrDefault());
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

                return this.context.OrchestratorActions;
            }
            finally
            {
                this.orchestrationRuntimeState.Status = this.taskOrchestration.GetStatus();
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