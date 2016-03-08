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

namespace DurableTask
{
    using System;
    using System.Reflection;

    /// <summary>
    ///     Allows users to load the TaskOrchestration and TaskActivity classes and start
    ///     dispatching to these. Also allows CRUD operations on the Task Hub itself.
    /// </summary>
    public sealed class TaskHubWorker2
    {
        readonly NameVersionObjectManager<TaskActivity> activityManager;
        readonly NameVersionObjectManager<TaskOrchestration> orchestrationManager;

        readonly object thisLock = new object();

        readonly TaskHubWorkerSettings workerSettings;
        public readonly IOrchestrationService orchestrationService;

        volatile bool isStarted;

        // AFFANDAR : TODO : replace with TaskActivityDispatcher2
        TaskActivityDispatcher2 activityDispatcher;
        TaskOrchestrationDispatcher2 orchestrationDispatcher;

        /// <summary>
        ///     Create a new TaskHubWorker with the given OrchestrationService with default settings.
        /// </summary>
        /// <param name="orchestrationService">Object implementing the <see cref="IOrchestrationService"/> interface </param>
        public TaskHubWorker2(IOrchestrationService orchestrationService)
            : this(orchestrationService, new TaskHubWorkerSettings())
        {
        }

        /// <summary>
        ///     Create a new TaskHubWorker with given OrchestrationService and settings
        /// </summary>
        /// <param name="orchestrationService">Reference the orchestration service implmentaion</param>
        /// <param name="workerSettings">Settings for various task hub worker options</param>
        public TaskHubWorker2(IOrchestrationService orchestrationService, TaskHubWorkerSettings workerSettings)
        {
            if (orchestrationService == null)
            {
                throw new ArgumentException("orchestrationService");
            }

            if (workerSettings == null)
            {
                throw new ArgumentException("workerSettings");
            }

            this.orchestrationManager = new NameVersionObjectManager<TaskOrchestration>();
            this.activityManager = new NameVersionObjectManager<TaskActivity>();
            this.orchestrationService = orchestrationService;
            this.workerSettings = workerSettings;
        }

        public TaskOrchestrationDispatcher2 TaskOrchestrationDispatcher
        {
            get { return orchestrationDispatcher; }
        }

        public TaskActivityDispatcher2 TaskActivityDispatcher
        {
            get { return activityDispatcher; }
        }

        // AFFANDAR : TODO : is this useful at all?

        ///// <summary>
        /////     Gets the TaskHubDescription for the configured TaskHub
        ///// </summary>
        ///// <returns></returns>
        //public TaskHubDescription GetTaskHubDescription()
        //{
        //    return Utils.AsyncExceptionWrapper(() => GetTaskHubDescriptionAsync().Result);
        //}

        ///// <summary>
        /////     Gets the TaskHubDescription for the configured TaskHub
        ///// </summary>
        ///// <returns></returns>
        //public async Task<TaskHubDescription> GetTaskHubDescriptionAsync()
        //{
        //    NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

        //    var description = new TaskHubDescription();

        //    IEnumerable<QueueDescription> queueDescriptions =
        //        await namespaceManager.GetQueuesAsync("startswith(path, '" + hubName + "') eq TRUE");

        //    List<QueueDescription> descriptions = queueDescriptions.ToList();

        //    QueueDescription orchestratorQueueDescription =
        //        descriptions.Single(q => string.Equals(q.Path, orchestratorEntityName));
        //    QueueDescription activityQueueDescription = descriptions.Single(q => string.Equals(q.Path, workerEntityName));
        //    QueueDescription trackingQueueDescription =
        //        descriptions.Single(q => string.Equals(q.Path, trackingEntityName));

        //    description.MaxTaskOrchestrationDeliveryCount = orchestratorQueueDescription.MaxDeliveryCount;
        //    description.MaxTaskActivityDeliveryCount = activityQueueDescription.MaxDeliveryCount;
        //    description.MaxTrackingDeliveryCount = trackingQueueDescription.MaxDeliveryCount;

        //    return description;
        //}

        /// <summary>
        ///     Starts the TaskHubWorker so it begins processing orchestrations and activities
        /// </summary>
        /// <returns></returns>
        public TaskHubWorker2 Start()
        {
            lock (thisLock)
            {
                if (isStarted)
                {
                    throw new InvalidOperationException("Worker is already started");
                }

                this.orchestrationDispatcher = new TaskOrchestrationDispatcher2(this.workerSettings, this.orchestrationService, this.orchestrationManager);
                this.activityDispatcher = new TaskActivityDispatcher2(this.workerSettings, this.orchestrationService, this.activityManager);

                this.orchestrationService.StartAsync().Wait();
                // AFFANDAR : TODO : make dispatcher start/stop methods async
                this.orchestrationDispatcher.Start();
                this.activityDispatcher.Start();


                isStarted = true;
            }

            return this;
        }

        /// <summary>
        ///     Gracefully stops the TaskHubWorker
        /// </summary>
        public void Stop()
        {
            Stop(false);
        }

        /// <summary>
        ///     Stops the TaskHubWorker
        /// </summary>
        /// <param name="isForced">True if forced shutdown, false if graceful shutdown</param>
        public void Stop(bool isForced)
        {
            lock (thisLock)
            {
                if (isStarted)
                {
                    this.orchestrationDispatcher.Stop(isForced);
                    this.activityDispatcher.Stop(isForced);
                    this.orchestrationService.StopAsync().Wait();

                    isStarted = false;
                }
            }
        }

        /// <summary>
        ///     Loads user defined TaskOrchestration classes in the TaskHubWorker
        /// </summary>
        /// <param name="taskOrchestrationTypes">Types deriving from TaskOrchestration class</param>
        /// <returns></returns>
        public TaskHubWorker2 AddTaskOrchestrations(params Type[] taskOrchestrationTypes)
        {
            foreach (Type type in taskOrchestrationTypes)
            {
                ObjectCreator<TaskOrchestration> creator = new DefaultObjectCreator<TaskOrchestration>(type);
                orchestrationManager.Add(creator);
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
        /// <returns></returns>
        public TaskHubWorker2 AddTaskOrchestrations(params ObjectCreator<TaskOrchestration>[] taskOrchestrationCreators)
        {
            foreach (var creator in taskOrchestrationCreators)
            {
                orchestrationManager.Add(creator);
            }

            return this;
        }

        /// <summary>
        ///     Loads user defined TaskActivity objects in the TaskHubWorker
        /// </summary>
        /// <param name="taskActivityObjects">Objects of with TaskActivity base type</param>
        /// <returns></returns>
        public TaskHubWorker2 AddTaskActivities(params TaskActivity[] taskActivityObjects)
        {
            foreach (TaskActivity instance in taskActivityObjects)
            {
                ObjectCreator<TaskActivity> creator = new DefaultObjectCreator<TaskActivity>(instance);
                activityManager.Add(creator);
            }

            return this;
        }

        /// <summary>
        ///     Loads user defined TaskActivity classes in the TaskHubWorker
        /// </summary>
        /// <param name="taskActivityTypes">Types deriving from TaskOrchestration class</param>
        /// <returns></returns>
        public TaskHubWorker2 AddTaskActivities(params Type[] taskActivityTypes)
        {
            foreach (Type type in taskActivityTypes)
            {
                ObjectCreator<TaskActivity> creator = new DefaultObjectCreator<TaskActivity>(type);
                activityManager.Add(creator);
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
        /// <returns></returns>
        public TaskHubWorker2 AddTaskActivities(params ObjectCreator<TaskActivity>[] taskActivityCreators)
        {
            foreach (var creator in taskActivityCreators)
            {
                activityManager.Add(creator);
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
        /// <returns></returns>
        public TaskHubWorker2 AddTaskActivitiesFromInterface<T>(T activities)
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
        /// <returns></returns>
        public TaskHubWorker2 AddTaskActivitiesFromInterface<T>(T activities, bool useFullyQualifiedMethodNames)
        {
            Type @interface = typeof (T);
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
                activityManager.Add(creator);
            }
            return this;
        }
    }
}