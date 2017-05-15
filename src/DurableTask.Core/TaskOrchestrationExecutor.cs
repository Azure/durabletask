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
        Task<string> result;

        public TaskOrchestrationExecutor(OrchestrationRuntimeState orchestrationRuntimeState,
            TaskOrchestration taskOrchestration)
        {
            decisionScheduler = new SynchronousTaskScheduler();
            context = new TaskOrchestrationContext(orchestrationRuntimeState.OrchestrationInstance, decisionScheduler);
            this.orchestrationRuntimeState = orchestrationRuntimeState;
            this.taskOrchestration = taskOrchestration;
        }

        public IEnumerable<OrchestratorAction> Execute()
        {
            SynchronizationContext prevCtx = SynchronizationContext.Current;

            try
            {
                SynchronizationContext syncCtx = new TaskOrchestrationSynchronizationContext(decisionScheduler);
                SynchronizationContext.SetSynchronizationContext(syncCtx);

                try
                {
                    foreach (HistoryEvent historyEvent in orchestrationRuntimeState.Events)
                    {
                        if (historyEvent.EventType == EventType.OrchestratorStarted)
                        {
                            var decisionStartedEvent = (OrchestratorStartedEvent) historyEvent;
                            context.CurrentUtcDateTime = decisionStartedEvent.Timestamp;
                            continue;
                        }

                        context.IsReplaying = historyEvent.IsPlayed;
                        ProcessEvent(historyEvent);
                        historyEvent.IsPlayed = true;
                    }

                    // check if workflow is completed after this replay
                    if (!context.HasOpenTasks)
                    {
                        if (result.IsCompleted)
                        {
                            if (result.IsFaulted)
                            {
                                context.FailOrchestration(result.Exception.InnerExceptions.FirstOrDefault());
                            }
                            else
                            {
                                context.CompleteOrchestration(result.Result);
                            }
                        }

                        // TODO: It is an error if result is not completed when all OpenTasks are done.
                        // Throw an exception in that case.
                    }
                }
                catch (NonDeterministicOrchestrationException exception)
                {
                    context.FailOrchestration(exception);
                }

                return context.OrchestratorActions;
            }
            finally
            {
                orchestrationRuntimeState.Status = taskOrchestration.GetStatus();
                SynchronizationContext.SetSynchronizationContext(prevCtx);
            }
        }

        void ProcessEvent(HistoryEvent historyEvent)
        {
            switch (historyEvent.EventType)
            {
                case EventType.ExecutionStarted:
                    var executionStartedEvent = (ExecutionStartedEvent) historyEvent;
                    result = taskOrchestration.Execute(context, executionStartedEvent.Input);
                    break;
                case EventType.ExecutionTerminated:
                    context.HandleExecutionTerminatedEvent((ExecutionTerminatedEvent) historyEvent);
                    break;
                case EventType.TaskScheduled:
                    context.HandleTaskScheduledEvent((TaskScheduledEvent) historyEvent);
                    break;
                case EventType.TaskCompleted:
                    context.HandleTaskCompletedEvent((TaskCompletedEvent) historyEvent);
                    break;
                case EventType.TaskFailed:
                    context.HandleTaskFailedEvent((TaskFailedEvent) historyEvent);
                    break;
                case EventType.SubOrchestrationInstanceCreated:
                    context.HandleSubOrchestrationCreatedEvent((SubOrchestrationInstanceCreatedEvent) historyEvent);
                    break;
                case EventType.SubOrchestrationInstanceCompleted:
                    context.HandleSubOrchestrationInstanceCompletedEvent(
                        (SubOrchestrationInstanceCompletedEvent) historyEvent);
                    break;
                case EventType.SubOrchestrationInstanceFailed:
                    context.HandleSubOrchestrationInstanceFailedEvent((SubOrchestrationInstanceFailedEvent) historyEvent);
                    break;
                case EventType.TimerCreated:
                    context.HandleTimerCreatedEvent((TimerCreatedEvent) historyEvent);
                    break;
                case EventType.TimerFired:
                    context.HandleTimerFiredEvent((TimerFiredEvent) historyEvent);
                    break;
                case EventType.EventRaised:
                    var eventRaisedEvent = (EventRaisedEvent) historyEvent;
                    taskOrchestration.RaiseEvent(context, eventRaisedEvent.Name, eventRaisedEvent.Input);
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