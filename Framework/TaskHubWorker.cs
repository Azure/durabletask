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
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Threading.Tasks;
    using DurableTask.Common;
    using DurableTask.Tracking;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;

    /// <summary>
    ///     Allows users to load the TaskOrchestration and TaskActivity classes and start
    ///     dispatching to these. Also allows CRUD operations on the Task Hub itself.
    /// </summary>
    //[Obsolete]
    public sealed class TaskHubWorker
    {
        readonly NameVersionObjectManager<TaskActivity> activityManager;
        readonly string connectionString;
        readonly string hubName;
        readonly NameVersionObjectManager<TaskOrchestration> orchestrationManager;
        readonly string orchestratorEntityName;
        readonly string tableStoreConnectionString;

        readonly object thisLock = new object();
        readonly string trackingEntityName;
        readonly string workerEntityName;

        readonly TaskHubWorkerSettings workerSettings;
        TaskActivityDispatcher activityDispatcher;
        volatile bool isStarted;
        TaskOrchestrationDispatcher orchestrationDispatcher;

        /// <summary>
        ///     Create a new TaskHubWorker with given name and Service Bus connection string
        /// </summary>
        /// <param name="hubName">Name of the Task Hub</param>
        /// <param name="connectionString">Service Bus connection string</param>
        public TaskHubWorker(string hubName, string connectionString)
            : this(hubName, connectionString, null, new TaskHubWorkerSettings())
        {
        }

        /// <summary>
        ///     Create a new TaskHubWorker with given name and Service Bus connection string
        /// </summary>
        /// <param name="hubName">Name of the Task Hub</param>
        /// <param name="connectionString">Service Bus connection string</param>
        /// <param name="workerSettings">configuration for task hub options</param>
        public TaskHubWorker(string hubName, string connectionString, TaskHubWorkerSettings workerSettings)
            : this(hubName, connectionString, null, workerSettings)
        {
        }

        /// <summary>
        ///     Create a new TaskHubWorker with given name, Service Bus and Azure Storage connection string
        /// </summary>
        /// <param name="hubName">Name of the Task Hub</param>
        /// <param name="connectionString">Service Bus connection string</param>
        /// <param name="tableStoreConnectionString">Azure Storage connection string</param>
        public TaskHubWorker(string hubName, string connectionString, string tableStoreConnectionString)
            : this(hubName, connectionString, tableStoreConnectionString, new TaskHubWorkerSettings())
        {
        }

        /// <summary>
        ///     Create a new TaskHubWorker with given name, Service Bus and Azure Storage connection string
        /// </summary>
        /// <param name="hubName">Name of the Task Hub</param>
        /// <param name="connectionString">Service Bus connection string</param>
        /// <param name="tableStoreConnectionString">Azure Storage connection string</param>
        /// <param name="workerSettings">Settings for various task hub worker options</param>
        public TaskHubWorker(string hubName, string connectionString, string tableStoreConnectionString,
            TaskHubWorkerSettings workerSettings)
        {
            if (string.IsNullOrEmpty(hubName))
            {
                throw new ArgumentException("hubName");
            }

            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentException("connectionString");
            }

            if (workerSettings == null)
            {
                throw new ArgumentException("workerSettings");
            }

            this.hubName = hubName.ToLower();
            this.connectionString = connectionString;
            workerEntityName = string.Format(FrameworkConstants.WorkerEndpointFormat, this.hubName);
            orchestratorEntityName = string.Format(FrameworkConstants.OrchestratorEndpointFormat, this.hubName);
            trackingEntityName = string.Format(FrameworkConstants.TrackingEndpointFormat, this.hubName);
            orchestrationManager = new NameVersionObjectManager<TaskOrchestration>();
            activityManager = new NameVersionObjectManager<TaskActivity>();
            this.tableStoreConnectionString = tableStoreConnectionString;
            this.workerSettings = workerSettings;
        }

        public TaskOrchestrationDispatcher TaskOrchestrationDispatcher
        {
            get { return orchestrationDispatcher; }
        }

        public TaskActivityDispatcher TaskActivityDispatcher
        {
            get { return activityDispatcher; }
        }

        /// <summary>
        ///     Gets the TaskHubDescription for the configured TaskHub
        /// </summary>
        /// <returns></returns>
        public TaskHubDescription GetTaskHubDescription()
        {
            return Utils.AsyncExceptionWrapper(() => GetTaskHubDescriptionAsync().Result);
        }

        /// <summary>
        ///     Gets the TaskHubDescription for the configured TaskHub
        /// </summary>
        /// <returns></returns>
        public async Task<TaskHubDescription> GetTaskHubDescriptionAsync()
        {
            NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

            var description = new TaskHubDescription();

            IEnumerable<QueueDescription> queueDescriptions =
                await namespaceManager.GetQueuesAsync("startswith(path, '" + hubName + "') eq TRUE");

            List<QueueDescription> descriptions = queueDescriptions.ToList();

            QueueDescription orchestratorQueueDescription =
                descriptions.Single(q => string.Equals(q.Path, orchestratorEntityName));
            QueueDescription activityQueueDescription = descriptions.Single(q => string.Equals(q.Path, workerEntityName));
            QueueDescription trackingQueueDescription =
                descriptions.Single(q => string.Equals(q.Path, trackingEntityName));

            description.MaxTaskOrchestrationDeliveryCount = orchestratorQueueDescription.MaxDeliveryCount;
            description.MaxTaskActivityDeliveryCount = activityQueueDescription.MaxDeliveryCount;
            description.MaxTrackingDeliveryCount = trackingQueueDescription.MaxDeliveryCount;

            return description;
        }

        /// <summary>
        ///     Starts the TaskHubWorker so it begins processing orchestrations and activities
        /// </summary>
        /// <returns></returns>
        public TaskHubWorker Start()
        {
            lock (thisLock)
            {
                if (isStarted)
                {
                    throw new InvalidOperationException("TaskHub is already started");
                }

                TaskHubDescription taskHubDescription = GetTaskHubDescription();

                orchestrationDispatcher =
                    new TaskOrchestrationDispatcher(ServiceBusUtils.CreateMessagingFactory(connectionString),
                        string.IsNullOrEmpty(tableStoreConnectionString)
                            ? null
                            : new TrackingDispatcher(
                                ServiceBusUtils.CreateMessagingFactory(connectionString),
                                taskHubDescription,
                                workerSettings,
                                tableStoreConnectionString, hubName,
                                trackingEntityName),
                        taskHubDescription,
                        workerSettings,
                        orchestratorEntityName, workerEntityName,
                        trackingEntityName, orchestrationManager);

                activityDispatcher =
                    new TaskActivityDispatcher(
                        ServiceBusUtils.CreateMessagingFactory(connectionString),
                        taskHubDescription,
                        workerSettings,
                        orchestratorEntityName,
                        workerEntityName,
                        activityManager);

                orchestrationDispatcher.Start();
                activityDispatcher.Start();

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
                    orchestrationDispatcher.Stop(isForced);
                    activityDispatcher.Stop(isForced);

                    isStarted = false;
                }
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
        public TaskHubWorker AddTaskOrchestrations(params ObjectCreator<TaskOrchestration>[] taskOrchestrationCreators)
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
        public TaskHubWorker AddTaskActivities(params TaskActivity[] taskActivityObjects)
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
        public TaskHubWorker AddTaskActivities(params Type[] taskActivityTypes)
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
        public TaskHubWorker AddTaskActivities(params ObjectCreator<TaskActivity>[] taskActivityCreators)
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
        /// <returns></returns>
        public TaskHubWorker AddTaskActivitiesFromInterface<T>(T activities, bool useFullyQualifiedMethodNames)
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

        // Management operations

        /// <summary>
        ///     Creates all the underlying entities in Service bus and Azure Storage (if specified) for
        ///     the TaskHubWorker and TaskHubClient's operations. If TaskHub already existed then
        ///     it would be deleted and recreated.
        /// </summary>
        public void CreateHub()
        {
            CreateHub(TaskHubDescription.CreateDefaultDescription(), true);
        }

        /// <summary>
        ///     Creates all the underlying entities in Service bus and Azure Storage (if specified) for
        ///     the TaskHubWorker and TaskHubClient's operations. If TaskHub already existed then
        ///     it would be deleted and recreated. Instance store creation can be controlled via parameter.
        /// </summary>
        public void CreateHub(bool createInstanceStore)
        {
            CreateHub(TaskHubDescription.CreateDefaultDescription(), createInstanceStore);
        }

        /// <summary>
        ///     Creates all the underlying entities in Service bus and Azure Storage (if specified) for
        ///     the TaskHubWorker and TaskHubClient's operations. If TaskHub already existed then
        ///     it would be deleted and recreated.
        /// </summary>
        public void CreateHub(TaskHubDescription description)
        {
            CreateHub(description, true);
        }

        /// <summary>
        ///     Creates all the underlying entities in Service bus and Azure Storage (if specified) for
        ///     the TaskHubWorker and TaskHubClient's operations. If TaskHub already existed then
        ///     it would be deleted and recreated. Instance store creation can be controlled via parameter.
        /// </summary>
        public void CreateHub(TaskHubDescription description, bool createInstanceStore)
        {
            NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

            SafeDeleteQueue(namespaceManager, orchestratorEntityName);
            SafeDeleteQueue(namespaceManager, workerEntityName);
            SafeDeleteQueue(namespaceManager, trackingEntityName);

            CreateQueue(namespaceManager, orchestratorEntityName, true, description.MaxTaskOrchestrationDeliveryCount);
            CreateQueue(namespaceManager, workerEntityName, false, description.MaxTaskActivityDeliveryCount);
            CreateQueue(namespaceManager, trackingEntityName, true, description.MaxTrackingDeliveryCount);

            if (!string.IsNullOrEmpty(tableStoreConnectionString) && createInstanceStore)
            {
                var client = new AzureTableClient(hubName, tableStoreConnectionString);
                client.DeleteTableIfExists();
                client.CreateTableIfNotExists();
            }
        }

        /// <summary>
        ///     Creates all the underlying entities in Service bus and Azure Storage (if specified) for
        ///     the TaskHubWorker and TaskHubClient's operations. If TaskHub already exists then this is a no-op
        /// </summary>
        public void CreateHubIfNotExists()
        {
            CreateHubIfNotExists(TaskHubDescription.CreateDefaultDescription());
        }

        /// <summary>
        ///     Creates all the underlying entities in Service bus and Azure Storage (if specified) for
        ///     the TaskHubWorker and TaskHubClient's operations. If TaskHub already exists then this is a no-op
        /// </summary>
        public void CreateHubIfNotExists(TaskHubDescription description)
        {
            NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

            IEnumerable<QueueDescription> queueDescriptions =
                namespaceManager.GetQueues("startswith(path, '" + hubName + "') eq TRUE");

            List<QueueDescription> descriptions = queueDescriptions.ToList();

            if (!descriptions.Any(q => string.Equals(q.Path, orchestratorEntityName)))
            {
                SafeCreateQueue(namespaceManager, orchestratorEntityName, true,
                    description.MaxTaskOrchestrationDeliveryCount);
            }

            if (!descriptions.Any(q => string.Equals(q.Path, workerEntityName)))
            {
                SafeCreateQueue(namespaceManager, workerEntityName, false, description.MaxTaskActivityDeliveryCount);
            }

            if (!descriptions.Any(q => string.Equals(q.Path, trackingEntityName)))
            {
                SafeCreateQueue(namespaceManager, trackingEntityName, true, description.MaxTrackingDeliveryCount);
            }

            if (!string.IsNullOrEmpty(tableStoreConnectionString))
            {
                var client = new AzureTableClient(hubName, tableStoreConnectionString);
                client.CreateTableIfNotExists();
            }
        }

        /// <summary>
        ///     Deletes all the underlying Service Bus and Azue Storage entities
        /// </summary>
        public void DeleteHub()
        {
            DeleteHub(true);
        }

        /// <summary>
        ///     Deletes all the underlying Service Bus entities.
        /// </summary>
        /// <param name="deleteInstanceStore">True if Azure Storage entities should also deleted, False otherwise.</param>
        public void DeleteHub(bool deleteInstanceStore)
        {
            NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

            SafeDeleteQueue(namespaceManager, orchestratorEntityName);
            SafeDeleteQueue(namespaceManager, workerEntityName);
            SafeDeleteQueue(namespaceManager, trackingEntityName);

            if (deleteInstanceStore)
            {
                if (!string.IsNullOrEmpty(tableStoreConnectionString))
                {
                    var client = new AzureTableClient(hubName, tableStoreConnectionString);
                    client.DeleteTableIfExists();
                }
            }
        }

        /// <summary>
        ///     Hub existence check.
        /// </summary>
        /// <returns>True if the Task Hub exists, false otherwise.</returns>
        public bool HubExists()
        {
            NamespaceManager namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);

            IEnumerable<QueueDescription> queueDescriptions =
                namespaceManager.GetQueues("startswith(path, '" + hubName + "') eq TRUE");

            if (!queueDescriptions.Where(q => string.Equals(q.Path, orchestratorEntityName)).Any())
            {
                return false;
            }

            // TODO : what if not all of the hub entities exist?

            return true;
        }

        static void SafeDeleteQueue(NamespaceManager namespaceManager, string path)
        {
            try
            {
                namespaceManager.DeleteQueue(path);
            }
            catch (MessagingEntityNotFoundException)
            {
            }
        }

        static void SafeCreateQueue(NamespaceManager namespaceManager, string path, bool requiresSessions,
            int maxDeliveryCount)
        {
            try
            {
                CreateQueue(namespaceManager, path, requiresSessions, maxDeliveryCount);
            }
            catch (MessagingEntityAlreadyExistsException)
            {
            }
        }

        static void CreateQueue(NamespaceManager namespaceManager, string path, bool requiresSessions,
            int maxDeliveryCount)
        {
            var description = new QueueDescription(path);
            description.RequiresSession = requiresSessions;
            description.MaxDeliveryCount = maxDeliveryCount;
            namespaceManager.CreateQueue(description);
        }
    }
}