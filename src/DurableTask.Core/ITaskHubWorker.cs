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
    using System;
    using System.Threading.Tasks;
    using DurableTask.Core.Middleware;

    /// <summary>
    /// TaskHubWorker interface for performing task hub worker operations
    /// </summary>
    public interface ITaskHubWorker : IDisposable
    {
        /// <summary>
        /// Gets the orchestration dispatcher
        /// </summary>
        TaskOrchestrationDispatcher TaskOrchestrationDispatcher { get; }

        /// <summary>
        /// Gets the task activity dispatcher
        /// </summary>
        TaskActivityDispatcher TaskActivityDispatcher { get; }

        /// <summary>
        /// Adds a middleware delegate to the orchestration dispatch pipeline.
        /// </summary>
        /// <param name="middleware">Delegate to invoke whenever a message is dispatched to an orchestration.</param>
        void AddOrchestrationDispatcherMiddleware(Func<DispatchMiddlewareContext, Func<Task>, Task> middleware);

        /// <summary>
        /// Adds a middleware delegate to the activity dispatch pipeline.
        /// </summary>
        /// <param name="middleware">Delegate to invoke whenever a message is dispatched to an activity.</param>
        void AddActivityDispatcherMiddleware(Func<DispatchMiddlewareContext, Func<Task>, Task> middleware);

        /// <summary>
        ///     Starts the TaskHubWorker so it begins processing orchestrations and activities
        /// </summary>
        /// <returns></returns>
        Task<ITaskHubWorker> StartAsync();

        /// <summary>
        ///     Gracefully stops the TaskHubWorker
        /// </summary>
        Task StopAsync();

        /// <summary>
        ///     Stops the TaskHubWorker
        /// </summary>
        /// <param name="isForced">True if forced shutdown, false if graceful shutdown</param>
        Task StopAsync(bool isForced);

        /// <summary>
        ///     Loads user defined TaskOrchestration classes in the TaskHubWorker
        /// </summary>
        /// <param name="taskOrchestrationTypes">Types deriving from TaskOrchestration class</param>
        /// <returns></returns>
        ITaskHubWorker AddTaskOrchestrations(params Type[] taskOrchestrationTypes);

        /// <summary>
        ///     Loads user defined TaskOrchestration classes in the TaskHubWorker
        /// </summary>
        /// <param name="taskOrchestrationCreators">
        ///     User specified ObjectCreators that will
        ///     create classes deriving TaskOrchestrations with specific names and versions
        /// </param>
        ITaskHubWorker AddTaskOrchestrations(params ObjectCreator<TaskOrchestration>[] taskOrchestrationCreators);

        /// <summary>
        ///     Loads user defined TaskActivity objects in the TaskHubWorker
        /// </summary>
        /// <param name="taskActivityObjects">Objects of with TaskActivity base type</param>
        ITaskHubWorker AddTaskActivities(params TaskActivity[] taskActivityObjects);

        /// <summary>
        ///     Loads user defined TaskActivity classes in the TaskHubWorker
        /// </summary>
        /// <param name="taskActivityTypes">Types deriving from TaskOrchestration class</param>
        ITaskHubWorker AddTaskActivities(params Type[] taskActivityTypes);

        /// <summary>
        ///     Loads user defined TaskActivity classes in the TaskHubWorker
        /// </summary>
        /// <param name="taskActivityCreators">
        ///     User specified ObjectCreators that will
        ///     create classes deriving TaskActivity with specific names and versions
        /// </param>
        ITaskHubWorker AddTaskActivities(params ObjectCreator<TaskActivity>[] taskActivityCreators);

        /// <summary>
        ///     Infers and adds every method in the specified interface T on the
        ///     passed in object as a different TaskActivity with Name set to the method name
        ///     and version set to an empty string. Methods can then be invoked from task orchestrations
        ///     by calling ScheduleTask(name, version) with name as the method name and string.Empty as the version.
        /// </summary>
        /// <typeparam name="T">Interface</typeparam>
        /// <param name="activities">Object that implements this interface</param>
        ITaskHubWorker AddTaskActivitiesFromInterface<T>(T activities);

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
        ITaskHubWorker AddTaskActivitiesFromInterface<T>(T activities, bool useFullyQualifiedMethodNames);
    }
}