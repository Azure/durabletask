﻿//  ----------------------------------------------------------------------------------
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
    using System;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core.Middleware;

    /// <summary>
    ///     Allows users to load the TaskOrchestration and TaskActivity classes and start
    ///     dispatching to these. Also allows CRUD operations on the Task Hub itself.
    /// </summary>
    public sealed class TaskHubWorker : IDisposable
    {
        readonly INameVersionObjectManager<TaskActivity> activityManager;
        readonly INameVersionObjectManager<TaskOrchestration> orchestrationManager;

        readonly DispatchMiddlewarePipeline orchestrationDispatchPipeline = new DispatchMiddlewarePipeline();
        readonly DispatchMiddlewarePipeline activityDispatchPipeline = new DispatchMiddlewarePipeline();

        readonly SemaphoreSlim slimLock = new SemaphoreSlim(1, 1);

        /// <summary>
        /// Reference to the orchestration service used by the task hub worker
        /// </summary>
        // ReSharper disable once InconsistentNaming (avoid breaking change)
        public IOrchestrationService orchestrationService { get; }

        volatile bool isStarted;

        TaskActivityDispatcher activityDispatcher;
        TaskOrchestrationDispatcher orchestrationDispatcher;

        /// <summary>
        ///     Create a new TaskHubWorker with given OrchestrationService
        /// </summary>
        /// <param name="orchestrationService">Reference the orchestration service implementation</param>
        public TaskHubWorker(IOrchestrationService orchestrationService)
            : this(
                  orchestrationService,
                  new NameVersionObjectManager<TaskOrchestration>(),
                  new NameVersionObjectManager<TaskActivity>())
        {
        }

        /// <summary>
        ///     Create a new TaskHubWorker with given OrchestrationService and name version managers
        /// </summary>
        /// <param name="orchestrationService">Reference the orchestration service implementation</param>
        /// <param name="orchestrationObjectManager">NameVersionObjectManager for Orchestrations</param>
        /// <param name="activityObjectManager">NameVersionObjectManager for Activities</param>
        public TaskHubWorker(
            IOrchestrationService orchestrationService,
            INameVersionObjectManager<TaskOrchestration> orchestrationObjectManager,
            INameVersionObjectManager<TaskActivity> activityObjectManager)
        {
            this.orchestrationManager = orchestrationObjectManager ?? throw new ArgumentException("orchestrationObjectManager");
            this.activityManager = activityObjectManager ?? throw new ArgumentException("activityObjectManager");
            this.orchestrationService = orchestrationService ?? throw new ArgumentException("orchestrationService");
        }

        /// <summary>
        /// Gets the orchestration dispatcher
        /// </summary>
        public TaskOrchestrationDispatcher TaskOrchestrationDispatcher => this.orchestrationDispatcher;

        /// <summary>
        /// Gets the task activity dispatcher
        /// </summary>
        public TaskActivityDispatcher TaskActivityDispatcher => this.activityDispatcher;

        /// <summary>
        /// Adds a middleware delegate to the orchestration dispatch pipeline.
        /// </summary>
        /// <param name="middleware">Delegate to invoke whenever a message is dispatched to an orchestration.</param>
        public void AddOrchestrationDispatcherMiddleware(Func<DispatchMiddlewareContext, Func<Task>, Task> middleware)
        {
            this.orchestrationDispatchPipeline.Add(middleware ?? throw new ArgumentNullException(nameof(middleware)));
        }

        /// <summary>
        /// Adds a middleware delegate to the activity dispatch pipeline.
        /// </summary>
        /// <param name="middleware">Delegate to invoke whenever a message is dispatched to an activity.</param>
        public void AddActivityDispatcherMiddleware(Func<DispatchMiddlewareContext, Func<Task>, Task> middleware)
        {
            this.activityDispatchPipeline.Add(middleware ?? throw new ArgumentNullException(nameof(middleware)));
        }

        /// <summary>
        ///     Starts the TaskHubWorker so it begins processing orchestrations and activities
        /// </summary>
        /// <returns></returns>
        public async Task<TaskHubWorker> StartAsync()
        {
            await this.slimLock.WaitAsync();
            try
            {
                if (this.isStarted)
                {
                    throw new InvalidOperationException("Worker is already started");
                }

                this.orchestrationDispatcher = new TaskOrchestrationDispatcher(
                    orchestrationService,
                    this.orchestrationManager,
                    this.orchestrationDispatchPipeline);
                this.activityDispatcher = new TaskActivityDispatcher(
                    orchestrationService,
                    this.activityManager,
                    this.activityDispatchPipeline);

                await orchestrationService.StartAsync();
                await this.orchestrationDispatcher.StartAsync();
                await this.activityDispatcher.StartAsync();

                this.isStarted = true;
            }
            finally
            {
                this.slimLock.Release();
            }

            return this;
        }

        /// <summary>
        ///     Gracefully stops the TaskHubWorker
        /// </summary>
        public async Task StopAsync()
        {
            await StopAsync(false);
        }

        /// <summary>
        ///     Stops the TaskHubWorker
        /// </summary>
        /// <param name="isForced">True if forced shutdown, false if graceful shutdown</param>
        public async Task StopAsync(bool isForced)
        {
            await this.slimLock.WaitAsync();
            try
            {
                if (this.isStarted)
                {
                    await this.orchestrationDispatcher.StopAsync(isForced);
                    await this.activityDispatcher.StopAsync(isForced);
                    await orchestrationService.StopAsync(isForced);

                    this.isStarted = false;
                }
            }
            finally
            {
                this.slimLock.Release();
            }
        }

        /// <summary>
        ///     Loads user defined TaskOrchestration classes in the TaskHubWorker
        /// </summary>
        /// <param name="taskOrchestrationTypes">Types deriving from TaskOrchestration class</param>
        /// <returns></returns>
        public TaskHubWorker AddTaskOrchestrations(params Type[] taskOrchestrationTypes)
        {
            foreach (Type type in taskOrchestrationTypes)
            {
                ObjectCreator<TaskOrchestration> creator = new DefaultObjectCreator<TaskOrchestration>(type);
                this.orchestrationManager.Add(creator);
            }

            return this;
        }

        /// <summary>
        ///     Loads user defined TaskOrchestration classes in the TaskHubWorker
        /// </summary>
        /// <param name="taskOrchestrationCreators">
        ///     User specified ObjectCreators that will
        ///     create classes deriving TaskOrchestrations with specific names and versions
        /// </param>
        public TaskHubWorker AddTaskOrchestrations(params ObjectCreator<TaskOrchestration>[] taskOrchestrationCreators)
        {
            foreach (ObjectCreator<TaskOrchestration> creator in taskOrchestrationCreators)
            {
                this.orchestrationManager.Add(creator);
            }

            return this;
        }

        /// <summary>
        ///     Loads user defined TaskActivity objects in the TaskHubWorker
        /// </summary>
        /// <param name="taskActivityObjects">Objects of with TaskActivity base type</param>
        public TaskHubWorker AddTaskActivities(params TaskActivity[] taskActivityObjects)
        {
            foreach (TaskActivity instance in taskActivityObjects)
            {
                ObjectCreator<TaskActivity> creator = new DefaultObjectCreator<TaskActivity>(instance);
                this.activityManager.Add(creator);
            }

            return this;
        }

        /// <summary>
        ///     Loads user defined TaskActivity classes in the TaskHubWorker
        /// </summary>
        /// <param name="taskActivityTypes">Types deriving from TaskOrchestration class</param>
        public TaskHubWorker AddTaskActivities(params Type[] taskActivityTypes)
        {
            foreach (Type type in taskActivityTypes)
            {
                ObjectCreator<TaskActivity> creator = new DefaultObjectCreator<TaskActivity>(type);
                this.activityManager.Add(creator);
            }

            return this;
        }

        /// <summary>
        ///     Loads user defined TaskActivity classes in the TaskHubWorker
        /// </summary>
        /// <param name="taskActivityCreators">
        ///     User specified ObjectCreators that will
        ///     create classes deriving TaskActivity with specific names and versions
        /// </param>
        public TaskHubWorker AddTaskActivities(params ObjectCreator<TaskActivity>[] taskActivityCreators)
        {
            foreach (ObjectCreator<TaskActivity> creator in taskActivityCreators)
            {
                this.activityManager.Add(creator);
            }

            return this;
        }

        /// <summary>
        ///     Infers and adds every method in the specified interface T on the
        ///     passed in object as a different TaskActivity with Name set to the method name
        ///     and version set to an empty string. Methods can then be invoked from task orchestrations
        ///     by calling ScheduleTask(name, version) with name as the method name and string.Empty as the version.
        /// </summary>
        /// <typeparam name="T">Interface</typeparam>
        /// <param name="activities">Object that implements this interface</param>
        public TaskHubWorker AddTaskActivitiesFromInterface<T>(T activities)
        {
            return AddTaskActivitiesFromInterface(activities, false);
        }

        /// <summary>
        ///     Infers and adds every method in the specified interface T on the
        ///     passed in object as a different TaskActivity with Name set to the method name
        ///     and version set to an empty string. Methods can then be invoked from task orchestrations
        ///     by calling ScheduleTask(name, version) with name as the method name and string.Empty as the version.
        /// </summary>
        /// <typeparam name="T">Interface</typeparam>
        /// <param name="activities">Object that implements this interface</param>
        /// <param name="useFullyQualifiedMethodNames">
        ///     If true, the method name translation from the interface contains
        ///     the interface name, if false then only the method name is used
        /// </param>
        public TaskHubWorker AddTaskActivitiesFromInterface<T>(T activities, bool useFullyQualifiedMethodNames)
        {
            Type @interface = typeof(T);
            if (!@interface.IsInterface)
            {
                throw new Exception("Contract can only be an interface.");
            }

            foreach (MethodInfo methodInfo in @interface.GetMethods())
            {
                TaskActivity taskActivity = new ReflectionBasedTaskActivity(activities, methodInfo);
                ObjectCreator<TaskActivity> creator =
                    new NameValueObjectCreator<TaskActivity>(
                        NameVersionHelper.GetDefaultName(methodInfo, useFullyQualifiedMethodNames),
                        NameVersionHelper.GetDefaultVersion(methodInfo), taskActivity);
                this.activityManager.Add(creator);
            }

            return this;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            ((IDisposable)this.slimLock).Dispose();
        }
    }
}