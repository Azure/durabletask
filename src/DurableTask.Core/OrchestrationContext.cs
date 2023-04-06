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
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Castle.DynamicProxy;
    using DurableTask.Core.Entities;
    using DurableTask.Core.Entities.OperationFormat;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.Serializing;

    /// <summary>
    /// Context for an orchestration containing the instance, replay status, orchestration methods and proxy methods
    /// </summary>
    public abstract class OrchestrationContext
    {
        /// <summary>
        /// Used in generating proxy interfaces and classes.
        /// </summary>
        private static readonly ProxyGenerator ProxyGenerator = new ProxyGenerator();

        /// <summary>
        /// Thread-static variable used to signal whether the calling thread is the orchestrator thread.
        /// The primary use case is for detecting illegal async usage in orchestration code.
        /// </summary>
        [ThreadStatic]
        public static bool IsOrchestratorThread;

        /// <summary>
        /// JsonDataConverter for message serialization settings
        /// </summary>
        public JsonDataConverter MessageDataConverter { get; set; }

        /// <summary>
        /// JsonDataConverter for error serialization settings
        /// </summary>
        public JsonDataConverter ErrorDataConverter { get; set; }

        /// <summary>
        /// Instance of the currently executing orchestration
        /// </summary>
        public OrchestrationInstance OrchestrationInstance { get; internal protected set; }

        /// <summary>
        /// Replay-safe current UTC datetime
        /// </summary>
        public virtual DateTime CurrentUtcDateTime { get; internal set; }

        /// <summary>
        ///     True if the code is currently replaying, False if code is truly executing for the first time.
        /// </summary>
        public bool IsReplaying { get; internal protected set; }

        /// <summary>
        /// Gets or sets a value indicating how to propagate unhandled exception metadata.
        /// </summary>
        internal ErrorPropagationMode ErrorPropagationMode { get; set; }

        /// <summary>
        /// Information about backend entity support, or null if the configured backend does not support entities.
        /// </summary>
        internal EntityBackendInformation EntityBackendInformation { get; set; }

        /// <summary>
        ///     Create a proxy client class to schedule remote TaskActivities via a strongly typed interface.
        /// </summary>
        /// <typeparam name="T">The interface for the proxy client</typeparam>
        /// <returns></returns>
        public virtual T CreateClient<T>() where T : class
        {
            return CreateClient<T>(false);
        }

        /// <summary>
        ///     Create a proxy client class to schedule remote TaskActivities via a strongly typed interface.
        /// </summary>
        /// <typeparam name="T">The interface for the proxy client</typeparam>
        /// <param name="useFullyQualifiedMethodNames">
        ///     If true, the method name translation from the interface contains
        ///     the interface name, if false then only the method name is used
        /// </param>
        /// <returns></returns>
        public virtual T CreateClient<T>(bool useFullyQualifiedMethodNames) where T : class
        {
            if (!typeof(T).IsInterface && !typeof(T).IsClass)
            {
                throw new InvalidOperationException($"{nameof(T)} must be an interface or class.");
            }

            IInterceptor scheduleProxy = new ScheduleProxy(this, useFullyQualifiedMethodNames);

            if (typeof(T).IsClass)
            {
                if (typeof(T).IsSealed)
                {
                    throw new InvalidOperationException("Class cannot be sealed.");
                }

                return ProxyGenerator.CreateClassProxy<T>(scheduleProxy);
            }

            return ProxyGenerator.CreateInterfaceProxyWithoutTarget<T>(scheduleProxy);
        }

        /// <summary>
        ///     Creates a proxy client with built-in retry logic.
        /// </summary>
        /// <typeparam name="T">
        ///     Task version of the client interface.
        ///     This is similar to the actual interface implemented by the client but with the
        ///     return types always of the form Task&lt;TOriginal&gt;
        ///     where TOriginal was the return
        ///     type for the same method in the original interface
        /// </typeparam>
        /// <param name="retryOptions">Retry policies</param>
        /// <returns>Dynamic proxy that can be used to schedule the remote tasks</returns>
        public virtual T CreateRetryableClient<T>(RetryOptions retryOptions) where T : class
        {
            return CreateRetryableClient<T>(retryOptions, false);
        }

        /// <summary>
        ///     Creates a proxy client with built-in retry logic.
        /// </summary>
        /// <typeparam name="T">
        ///     Task version of the client interface.
        ///     This is similar to the actual interface implemented by the client but with the
        ///     return types always of the form Task&lt;TOriginal&gt;
        ///     where TOriginal was the return
        ///     type for the same method in the original interface
        /// </typeparam>
        /// <param name="retryOptions">Retry policies</param>
        /// <param name="useFullyQualifiedMethodNames">
        ///     If true, the method name translation from the interface contains
        ///     the interface name, if false then only the method name is used
        /// </param>
        /// <returns>Dynamic proxy that can be used to schedule the remote tasks</returns>
        public virtual T CreateRetryableClient<T>(RetryOptions retryOptions, bool useFullyQualifiedMethodNames) where T : class
        {
            if (!typeof(T).IsInterface && !typeof(T).IsClass)
            {
                throw new InvalidOperationException($"{nameof(T)} must be an interface or class.");
            }

            IInterceptor scheduleProxy = new ScheduleProxy(this, useFullyQualifiedMethodNames);
            IInterceptor retryProxy = new RetryProxy(this, retryOptions);

            if (typeof(T).IsClass)
            {
                if (typeof(T).IsSealed)
                {
                    throw new InvalidOperationException($"Class cannot be sealed.");
                }

                return ProxyGenerator.CreateClassProxyWithTarget(target: ProxyGenerator.CreateClassProxy<T>(scheduleProxy), retryProxy);
            }

            T scheduleInstance = ProxyGenerator.CreateInterfaceProxyWithoutTarget<T>(scheduleProxy);
            return ProxyGenerator.CreateInterfaceProxyWithTarget(scheduleInstance, retryProxy);
        }

        /// <summary>
        ///     Schedule a TaskActivity by type. Also retry on failure as per supplied policy.
        /// </summary>
        /// <typeparam name="T">Return Type of the TaskActivity.Execute method</typeparam>
        /// <param name="taskActivityType">Type that devices from TaskActivity class</param>
        /// <param name="retryOptions">Retry policy</param>
        /// <param name="parameters">Parameters for the TaskActivity.Execute method</param>
        /// <returns>Task that represents the execution of the specified TaskActivity</returns>
        public virtual Task<T> ScheduleWithRetry<T>(Type taskActivityType, RetryOptions retryOptions,
            params object[] parameters)
        {
            return ScheduleWithRetry<T>(NameVersionHelper.GetDefaultName(taskActivityType),
                NameVersionHelper.GetDefaultVersion(taskActivityType),
                retryOptions, parameters);
        }

        /// <summary>
        ///     Schedule a TaskActivity by name and version. Also retry on failure as per supplied policy.
        /// </summary>
        /// <typeparam name="T">Return Type of the TaskActivity.Execute method</typeparam>
        /// <param name="name">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="version">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="retryOptions">Retry policy</param>
        /// <param name="parameters">Parameters for the TaskActivity.Execute method</param>
        /// <returns>Task that represents the execution of the specified TaskActivity</returns>
        public virtual Task<T> ScheduleWithRetry<T>(string name, string version, RetryOptions retryOptions,
            params object[] parameters)
        {
            Task<T> RetryCall() => ScheduleTask<T>(name, version, parameters);
            var retryInterceptor = new RetryInterceptor<T>(this, retryOptions, RetryCall);
            return retryInterceptor.Invoke();
        }

        /// <summary>
        ///     Create a sub-orchestration of the specified type. Also retry on failure as per supplied policy.
        /// </summary>
        /// <typeparam name="T">Return Type of the TaskOrchestration.RunTask method</typeparam>
        /// <param name="orchestrationType">Type of the TaskOrchestration derived class to instantiate</param>
        /// <param name="retryOptions">Retry policy</param>
        /// <param name="input">Input for the TaskOrchestration.RunTask method</param>
        /// <returns>Task that represents the execution of the specified sub-orchestration</returns>
        public virtual Task<T> CreateSubOrchestrationInstanceWithRetry<T>(Type orchestrationType,
            RetryOptions retryOptions, object input)
        {
            return CreateSubOrchestrationInstanceWithRetry<T>(NameVersionHelper.GetDefaultName(orchestrationType),
                NameVersionHelper.GetDefaultVersion(orchestrationType), retryOptions, input);
        }

        /// <summary>
        ///     Create a sub-orchestration of the specified type. Also retry on failure as per supplied policy.
        /// </summary>
        /// <typeparam name="T">Return Type of the TaskOrchestration.RunTask method</typeparam>
        /// <param name="orchestrationType">Type of the TaskOrchestration derived class to instantiate</param>
        /// <param name="instanceId">Instance Id of the sub-orchestration</param>
        /// <param name="retryOptions">Retry policy</param>
        /// <param name="input">Input for the TaskOrchestration.RunTask method</param>
        /// <returns>Task that represents the execution of the specified sub-orchestration</returns>
        public virtual Task<T> CreateSubOrchestrationInstanceWithRetry<T>(Type orchestrationType, string instanceId,
            RetryOptions retryOptions, object input)
        {
            return CreateSubOrchestrationInstanceWithRetry<T>(NameVersionHelper.GetDefaultName(orchestrationType),
                NameVersionHelper.GetDefaultVersion(orchestrationType), instanceId, retryOptions, input);
        }

        /// <summary>
        ///     Create a sub-orchestration of the specified name and version. Also retry on failure as per supplied policy.
        /// </summary>
        /// <typeparam name="T">Return Type of the TaskOrchestration.RunTask method</typeparam>
        /// <param name="name">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="version">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="retryOptions">Retry policy</param>
        /// <param name="input">Input for the TaskOrchestration.RunTask method</param>
        /// <returns>Task that represents the execution of the specified sub-orchestration</returns>
        public virtual Task<T> CreateSubOrchestrationInstanceWithRetry<T>(string name, string version,
            RetryOptions retryOptions, object input)
        {
            Task<T> RetryCall() => CreateSubOrchestrationInstance<T>(name, version, input);
            var retryInterceptor = new RetryInterceptor<T>(this, retryOptions, RetryCall);
            return retryInterceptor.Invoke();
        }

        /// <summary>
        ///     Create a sub-orchestration of the specified name and version. Also retry on failure as per supplied policy.
        /// </summary>
        /// <typeparam name="T">Return Type of the TaskOrchestration.RunTask method</typeparam>
        /// <param name="name">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="version">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="instanceId">Instance Id of the sub-orchestration</param>
        /// <param name="retryOptions">Retry policy</param>
        /// <param name="input">Input for the TaskOrchestration.RunTask method</param>
        /// <returns>Task that represents the execution of the specified sub-orchestration</returns>
        public virtual Task<T> CreateSubOrchestrationInstanceWithRetry<T>(string name, string version, string instanceId,
            RetryOptions retryOptions, object input)
        {
            Task<T> RetryCall() => CreateSubOrchestrationInstance<T>(name, version, instanceId, input);
            var retryInterceptor = new RetryInterceptor<T>(this, retryOptions, RetryCall);
            return retryInterceptor.Invoke();
        }

        /// <summary>
        ///     Schedule a TaskActivity by type.
        /// </summary>
        /// <typeparam name="TResult">Return Type of the TaskActivity.Execute method</typeparam>
        /// <param name="activityType">Type that devices from TaskActivity class</param>
        /// <param name="parameters">Parameters for the TaskActivity.Execute method</param>
        /// <returns>Task that represents the execution of the specified TaskActivity</returns>
        public virtual Task<TResult> ScheduleTask<TResult>(Type activityType, params object[] parameters)
        {
            return ScheduleTask<TResult>(NameVersionHelper.GetDefaultName(activityType),
                NameVersionHelper.GetDefaultVersion(activityType), parameters);
        }

        /// <summary>
        ///     Schedule a TaskActivity by name and version.
        /// </summary>
        /// <typeparam name="TResult">Return Type of the TaskActivity.Execute method</typeparam>
        /// <param name="name">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="version">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="parameters">Parameters for the TaskActivity.Execute method</param>
        /// <returns>Task that represents the execution of the specified TaskActivity</returns>
        public abstract Task<TResult> ScheduleTask<TResult>(string name, string version, params object[] parameters);

        /// <summary>
        ///     Create a timer that will fire at the specified time and hand back the specified state.
        /// </summary>
        /// <typeparam name="T">Type of state object</typeparam>
        /// <param name="fireAt">Absolute time at which the timer should fire</param>
        /// <param name="state">The state to be handed back when the timer fires</param>
        /// <returns>Task that represents the async wait on the timer</returns>
        public abstract Task<T> CreateTimer<T>(DateTime fireAt, T state);

        /// <summary>
        ///     Create a timer that will fire at the specified time and hand back the specified state.
        /// </summary>
        /// <typeparam name="T">Type of state object</typeparam>
        /// <param name="fireAt">Absolute time at which the timer should fire</param>
        /// <param name="state">The state to be handed back when the timer fires</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns>Task that represents the async wait on the timer</returns>
        public abstract Task<T> CreateTimer<T>(DateTime fireAt, T state, CancellationToken cancelToken);

        /// <summary>
        ///     Create a sub-orchestration of the specified type.
        /// </summary>
        /// <typeparam name="T">Return Type of the TaskOrchestration.RunTask method</typeparam>
        /// <param name="orchestrationType">Type of the TaskOrchestration derived class to instantiate</param>
        /// <param name="input">Input for the TaskOrchestration.RunTask method</param>
        /// <returns>Task that represents the execution of the specified sub-orchestration</returns>
        public virtual Task<T> CreateSubOrchestrationInstance<T>(Type orchestrationType, object input)
        {
            return CreateSubOrchestrationInstance<T>(NameVersionHelper.GetDefaultName(orchestrationType),
                NameVersionHelper.GetDefaultVersion(orchestrationType), input);
        }

        /// <summary>
        ///     Create a sub-orchestration of the specified type with the specified instance id
        /// </summary>
        /// <typeparam name="T">Return Type of the TaskOrchestration.RunTask method</typeparam>
        /// <param name="orchestrationType">Type of the TaskOrchestration derived class to instantiate</param>
        /// <param name="instanceId">InstanceId of the sub-orchestration to create</param>
        /// <param name="input">Input for the TaskOrchestration.RunTask method</param>
        /// <returns>Task that represents the execution of the specified sub-orchestration</returns>
        public virtual Task<T> CreateSubOrchestrationInstance<T>(Type orchestrationType, string instanceId, object input)
        {
            return CreateSubOrchestrationInstance<T>(NameVersionHelper.GetDefaultName(orchestrationType),
                NameVersionHelper.GetDefaultVersion(orchestrationType), instanceId, input);
        }

        /// <summary>
        ///     Create a sub-orchestration of the specified name and version.
        /// </summary>
        /// <typeparam name="T">Return Type of the TaskOrchestration.RunTask method</typeparam>
        /// <param name="name">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="version">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="input">Input for the TaskOrchestration.RunTask method</param>
        /// <returns>Task that represents the execution of the specified sub-orchestration</returns>
        public abstract Task<T> CreateSubOrchestrationInstance<T>(string name, string version, object input);

        /// <summary>
        ///     Create a sub-orchestration of the specified name and version with the specific instance id
        /// </summary>
        /// <typeparam name="T">Return Type of the TaskOrchestration.RunTask method</typeparam>
        /// <param name="name">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="version">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="instanceId">InstanceId of the sub-orchestration to create</param>
        /// <param name="input">Input for the TaskOrchestration.RunTask method</param>
        /// <returns>Task that represents the execution of the specified sub-orchestration</returns>
        public abstract Task<T> CreateSubOrchestrationInstance<T>(string name, string version, string instanceId,
            object input);

        /// <summary>
        ///     Create a sub-orchestration of the specified name and version with the specific instance id
        /// </summary>
        /// <typeparam name="T">Return Type of the TaskOrchestration.RunTask method</typeparam>
        /// <param name="name">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="version">Name of the orchestration as specified by the ObjectCreator</param>
        /// <param name="instanceId">InstanceId of the sub-orchestration to create</param>
        /// <param name="input">Input for the TaskOrchestration.RunTask method</param>
        /// <param name="tags">Dictionary of key/value tags associated with this instance</param>
        /// <returns>Task that represents the execution of the specified sub-orchestration</returns>
        public abstract Task<T> CreateSubOrchestrationInstance<T>(string name, string version, string instanceId,
            object input, IDictionary<string, string> tags);

        /// <summary>
        /// Calls an operation on an entity and returns the result asynchronously.
        /// </summary>
        /// <typeparam name="TResult">The result type of the operation.</typeparam>
        /// <param name="entityId">The target entity.</param>
        /// <param name="operationName">The name of the operation.</param>
        /// <param name="operationInput">The input for the operation.</param>
        /// <exception cref="EntityLockingRulesViolationException">if the orchestration is inside a critical section and the lock for this entity is not available.</exception>
        /// <returns>A task representing the result of the operation.</returns>
        public virtual Task<TResult> CallEntityAsync<TResult>(Entities.EntityId entityId, string operationName, object operationInput = null)
            => throw new NotImplementedException();

        /// <summary>
        /// Calls an operation on an entity and waits for it to complete.
        /// </summary>
        /// <param name="entityId">The target entity.</param>
        /// <param name="operationName">The name of the operation.</param>
        /// <param name="operationInput">The input for the operation.</param>
        /// <returns>A task representing the completion of the operation on the entity.</returns>
        /// <exception cref="EntityLockingRulesViolationException">if the orchestration is inside a critical section and the lock for this entity is not available.</exception>
        public virtual Task CallEntityAsync(Entities.EntityId entityId, string operationName, object operationInput = null)
            => throw new NotImplementedException();

        /// <summary>
        /// Signals an entity operation on the specified entity.
        /// </summary>    
        /// <param name="entityId">The entity ID of the target.</param>
        /// <param name="operationName">The name of the operation.</param>
        /// <param name="operationInput">The input for the operation.</param>
        /// <exception cref="EntityLockingRulesViolationException">if the orchestration is inside a critical section that locked this entity.</exception>
        public virtual void SignalEntity(Entities.EntityId entityId, string operationName, object operationInput = null)
            => throw new NotImplementedException();

        /// <summary>
        /// Signals an entity to perform an operation, at a specified time. Any result or exception is ignored (fire and forget).
        /// </summary>
        /// <param name="entityId">The entity ID of the target.</param>
        /// <param name="scheduledTimeUtc">The time at which to start the operation.</param>
        /// <param name="operationName">The name of the operation.</param>
        /// <exception cref="EntityLockingRulesViolationException">if the orchestration is inside a critical section that locked this entity.</exception>
        /// <param name="operationInput">The input for the operation.</param>
        public virtual void SignalEntity(Entities.EntityId entityId, DateTime scheduledTimeUtc, string operationName, object operationInput = null)
            => throw new NotImplementedException();

        /// <summary>
        /// Locks one or more entities for the duration of the critical section.
        /// </summary>
        /// <remarks>
        /// Locks can only be acquired if the current context is not already in a critical section.
        /// </remarks>
        /// <param name="entities">The entities whose locks should be acquired.</param>
        /// <returns>A task that must be awaited prior to entering the critical section, and which returns a IDisposable that must be disposed after exiting the critical section.</returns>
        /// <exception cref="EntityLockingRulesViolationException">if the context is already in a critical section.</exception>
        public virtual Task<IDisposable> LockEntitiesAsync(params EntityId[] entities)
            => throw new NotImplementedException();

        /// <summary>
        /// Whether this orchestration is currently inside a critical section. Critical sections are entered when calling
        /// <see cref="LockEntitiesAsync(EntityId[])"/>, and are exited when disposing the returned IDisposable.
        /// </summary>
        public virtual bool IsInsideCriticalSection { get { throw new NotImplementedException(); } }

        /// <summary>
        /// Enumerates all the entities that can be called from within the current critical section. 
        /// This set contains all the entities that were locked prior to entering the critical section,
        /// and for which there is not currently an operation call pending.
        /// </summary>
        /// <returns>An enumeration of all the currently available entities.</returns>
        public virtual IEnumerable<EntityId> GetAvailableEntities()
            => throw new NotImplementedException();

        /// <summary>
        ///     Raises an event for the specified orchestration instance, which eventually causes the OnEvent() method in the
        ///     orchestration to fire.
        /// </summary>
        /// <param name="orchestrationInstance">Instance in which to raise the event</param>
        /// <param name="eventName">Name of the event</param>
        /// <param name="eventData">Data for the event</param>
        public abstract void SendEvent(OrchestrationInstance orchestrationInstance, string eventName, object eventData);

        /// <summary>
        ///     Checkpoint the orchestration instance by completing the current execution in the ContinueAsNew
        ///     state and creating a new execution of this instance with the specified input parameter.
        ///     This is useful in unbounded workflows to ensure that the execution history gets cleaned up regularly and
        ///     does not overflow the preset size limit.
        /// </summary>
        /// <param name="input">
        ///     Input to the new execution of this instance. This is the same type as the one used to start
        ///     the first execution of this orchestration instance.
        /// </param>
        public abstract void ContinueAsNew(object input);

        /// <summary>
        ///     Checkpoint the orchestration instance by completing the current execution in the ContinueAsNew
        ///     state and creating a new execution of this instance with the specified input parameter.
        ///     This is useful in unbounded workflows to ensure that the execution history gets cleaned up regularly and
        ///     does not overflow the preset size limit.
        /// </summary>
        /// <param name="newVersion">
        ///     New version of the orchestration to start
        /// </param>
        /// <param name="input">
        ///     Input to the new execution of this instance. This is the same type as the one used to start
        ///     the first execution of this orchestration instance.
        /// </param>
        public abstract void ContinueAsNew(string newVersion, object input);
    }
}