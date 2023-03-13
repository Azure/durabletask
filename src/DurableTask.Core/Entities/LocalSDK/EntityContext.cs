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
    using DurableTask.Core.Entities;
    using System;
    using System.Reflection;
    using System.Threading.Tasks;

    /// <summary>
    /// Context for an entity, which is available to the application code while it is executing entity operations.
    /// </summary>
    /// <typeparam name="TState">The JSON-serializable type of the entity state.</typeparam>
    public abstract class EntityContext<TState>
    {
        /// <summary>
        /// Gets the name of the currently executing entity.
        /// </summary>
        public abstract string EntityName { get; }

        /// <summary>
        /// Gets the key of the currently executing entity.
        /// </summary>
        public abstract string EntityKey { get; }

        /// <summary>
        /// Gets the id of the currently executing entity.
        /// </summary>
        public abstract EntityId EntityId { get; }

        /// <summary>
        /// Gets the name of the operation that was called.
        /// </summary>
        /// <remarks>
        /// An operation invocation on an entity includes an operation name, which states what
        /// operation to perform, and optionally an operation input.
        /// </remarks>
        public abstract string OperationName { get; }

        /// <summary>
        /// Whether this entity has state. 
        /// </summary>
        /// <remarks>The value of <see cref="HasState"/> changes as entity operations access or delete the state during their execution. It is set to true after <see cref="State"/> is accessed from within an operation, 
        /// and it is set to false after <see cref="DeleteState"/> is called. This can happen repeatedly; neither creation nor deletion are permanent.</remarks>
        public abstract bool HasState { get; }

        /// <summary>
        /// The size of the current batch of operations.
        /// </summary>
        public abstract int BatchSize { get; }

        /// <summary>
        /// The position of the currently executing operation within the current batch of operations.
        /// </summary>
        public abstract int BatchPosition { get; }

        /// <summary>
        /// Gets or sets the current state of the entity. Implicitly creates the state if the entity does not have state yet.
        /// </summary>
        /// <remarks>If the entity does not have a state yet (first time access), 
        /// or does not have a state anymore (was deleted), accessing this property implicitly creates state for the entity. For the 'get' accessor, this means 
        /// that <see cref="TaskEntity{TState}.CreateInitialState(EntityContext{TState})"/> is called to create the state.
        /// For the 'set' accessor, the given value is used.</remarks>
        public abstract TState State { get; set; }

        /// <summary>
        /// Deletes the state of this entity.
        /// </summary>
        public abstract void DeleteState();

        /// <summary>
        /// Gets the input for this operation, as a deserialized value.
        /// </summary>
        /// <typeparam name="TInput">The JSON-serializable type used for the operation input.</typeparam>
        /// <returns>The operation input, or default(<typeparamref name="TInput"/>) if none.</returns>
        /// <remarks>
        /// An operation invocation on an entity includes an operation name, which states what
        /// operation to perform, and optionally an operation input.
        /// </remarks>
        public abstract TInput GetInput<TInput>();

        /// <summary>
        /// Gets the input for this operation, as a deserialized value.
        /// </summary>
        /// <param name="inputType">The JSON-serializable type used for the operation input.</param>
        /// <returns>The operation input, or default(<paramref name="inputType"/>) if none.</returns>
        /// <remarks>
        /// An operation invocation on an entity includes an operation name, which states what
        /// operation to perform, and optionally an operation input.
        /// </remarks>
        public abstract object GetInput(Type inputType);

        /// <summary>
        /// Returns the given result to the caller of this operation.
        /// </summary>
        /// <param name="result">the result to return.</param>
        public abstract void Return(object result);

        /// <summary>
        /// Signals an entity to perform an operation, without waiting for a response. Any result or exception is ignored (fire and forget).
        /// </summary>
        /// <param name="entity">The target entity.</param>
        /// <param name="operationName">The name of the operation.</param>
        /// <param name="operationInput">The operation input.</param>
        public abstract void SignalEntity(EntityId entity, string operationName, object operationInput = null);

        /// <summary>
        /// Signals an entity to perform an operation, at a specified time. Any result or exception is ignored (fire and forget).
        /// </summary>
        /// <param name="entity">The target entity.</param>
        /// <param name="scheduledTimeUtc">The time at which to start the operation.</param>
        /// <param name="operationName">The name of the operation.</param>
        /// <param name="operationInput">The input for the operation.</param>
        public abstract void SignalEntity(EntityId entity, DateTime scheduledTimeUtc, string operationName, object operationInput = null);

        /// <summary>
        /// Schedules an orchestration function with the given name and version for execution./>.
        /// Any result or exception is ignored (fire and forget).
        /// </summary>
        /// <param name="name">The name of the orchestrator, as specified by the ObjectCreator.</param>
        /// <param name="version">The version of the orchestrator.</param>
        /// <param name="input">the input to pass to the orchestrator function.</param>
        /// <param name="instanceId">optionally, an instance id for the orchestration. By default, a random GUID is used.</param>
        /// <returns>The instance id of the new orchestration.</returns>
        public abstract string StartNewOrchestration(string name, string version, object input, string instanceId = null);

        /// <summary>
        /// Schedules an orchestration function with the given type for execution./>.
        /// Any result or exception is ignored (fire and forget).
        /// </summary>
        /// <param name="orchestrationType">Type of the TaskOrchestration derived class to instantiate</param>
        /// <param name="input">the input to pass to the orchestrator function.</param>
        /// <param name="instanceId">optionally, an instance id for the orchestration. By default, a random GUID is used.</param>
        /// <returns>The instance id of the new orchestration.</returns>
        public virtual string StartNewOrchestration(Type orchestrationType, object input, string instanceId = null)
            => this.StartNewOrchestration(NameVersionHelper.GetDefaultName(orchestrationType), NameVersionHelper.GetDefaultVersion(orchestrationType), input, instanceId);
    }
}