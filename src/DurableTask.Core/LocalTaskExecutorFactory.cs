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
    using System.Diagnostics;
    using System.Threading.Tasks;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Middleware;
    using DurableTask.Core.Tracing;

    class LocalTaskExecutorFactory : ITaskExecutorFactory
    {
        readonly IOrchestrationService orchestrationService;
        readonly INameVersionObjectManager<TaskActivity> activityObjectManager;
        readonly INameVersionObjectManager<TaskOrchestration> orchestrationObjectManager;

        public LocalTaskExecutorFactory(
            IOrchestrationService orchestrationService,
            INameVersionObjectManager<TaskOrchestration> orchestrationObjectManager,
            INameVersionObjectManager<TaskActivity> activityObjectManager)
        {
            this.orchestrationService = orchestrationService ?? throw new ArgumentNullException(nameof(orchestrationService));
            this.orchestrationObjectManager = orchestrationObjectManager ?? throw new ArgumentNullException(nameof(orchestrationObjectManager));
            this.activityObjectManager = activityObjectManager ?? throw new ArgumentNullException(nameof(activityObjectManager));
        }

        public OrchestrationExecutorBase CreateOrchestrationExecutor(OrchestrationRuntimeState runtimeState)
        {
            TaskOrchestration taskOrchestration = this.orchestrationObjectManager.GetObject(runtimeState.Name, runtimeState.Version);
            if (taskOrchestration == null)
            {
                throw TraceHelper.TraceExceptionInstance(
                    TraceEventType.Error,
                    "TaskOrchestrationDispatcher-TypeMissing",
                    runtimeState.OrchestrationInstance,
                    new TypeMissingException($"Orchestration not found: ({runtimeState.Name}, {runtimeState.Version})"));
            }

            return new TaskOrchestrationExecutor(
                runtimeState,
                taskOrchestration,
                this.orchestrationService.EventBehaviourForContinueAsNew);
        }

        public TaskActivityExecutorBase CreateActivityExecutor(TaskScheduledEvent scheduledEvent, OrchestrationInstance instance)
        {
            TaskActivity taskActivity = this.activityObjectManager.GetObject(scheduledEvent.Name, scheduledEvent.Version);
            if (taskActivity == null)
            {
                throw new TypeMissingException($"TaskActivity {scheduledEvent.Name} version {scheduledEvent.Version} was not found");
            }

            var taskContext = new TaskContext(instance);
            return new LocalActivityExecutor(taskActivity, taskContext, scheduledEvent);
        }

        class LocalActivityExecutor : TaskActivityExecutorBase
        {
            readonly TaskActivity taskActivity;
            readonly TaskContext taskContext;
            readonly TaskScheduledEvent taskScheduledEvent;

            public LocalActivityExecutor(
                TaskActivity taskActivity,
                TaskContext taskContext,
                TaskScheduledEvent taskScheduledEvent)
            {
                this.taskActivity = taskActivity ?? throw new ArgumentNullException(nameof(taskActivity));
                this.taskContext = taskContext ?? throw new ArgumentNullException(nameof(taskContext));
                this.taskScheduledEvent = taskScheduledEvent ?? throw new ArgumentNullException(nameof(taskScheduledEvent));
            }

            public override Task<string?> ExecuteAsync()
            {
                return this.taskActivity.RunAsync(this.taskContext, this.taskScheduledEvent.Input);
            }

            protected internal override DispatchMiddlewareContext CreateDispatchContext()
            {
                DispatchMiddlewareContext middlewareContext = base.CreateDispatchContext();
                middlewareContext.SetProperty(this.taskActivity);
                return middlewareContext;
            }
        }
    }
}
