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

namespace DurableTask.Core.Entities
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using DurableTask.Core.Common;
    using DurableTask.Core.Entities.EventFormat;
    using DurableTask.Core.Entities.OperationFormat;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.Serializing;
    using Newtonsoft.Json;

    /// <summary>
    /// Base class for TaskEntity. 
    /// </summary>
    /// <remarks>For in-process user code, we recommend using the specialized
    /// class <see cref="TaskEntity{TState}"/> which has a per-operation interface,
    /// provides type-safe access to the state, and handles state management, including initialization,
    /// serialization, and deserialization.</remarks>
    public abstract class TaskEntity
    {
        /// <summary>
        /// Executes the given operation batch and return the results.
        /// </summary>
        /// <param name="operations">The batch of operations to execute.</param>
        /// <param name="options">Options to control entity execution.</param>
        /// <returns></returns>
        public abstract Task<OperationBatchResult> ExecuteOperationBatchAsync(OperationBatchRequest operations, EntityExecutionOptions options);
    }

    /// <summary>
    /// TaskEntity representing an entity with typed state and options for controlling serialization and batch execution.
    /// </summary>
    /// <typeparam name="TState">The type of the entity state.</typeparam>
    public abstract class TaskEntity<TState> : TaskEntity
    {
        /// <summary>
        /// Defines how this entity processes operations. Implementations must override this.
        /// </summary>
        /// <param name="context">The context for the operation.</param>
        /// <returns>a task that indicates completion of the operation</returns>
        public abstract ValueTask ExecuteOperationAsync(EntityContext<TState> context);

        /// <summary>
        /// A function for creating the initial state of the entity. This is 
        /// automatically called when the entity is first accessed, or 
        /// when it is accessed after having been deleted.
        /// Implementations may override this if they want to perform a different initialization.
        /// </summary>
        public virtual TState CreateInitialState(EntityContext<TState> context)
        {
            return default(TState);
        }

        /// <summary>
        /// The data converter used for converting inputs and outputs for operations.
        /// Implementations may override this setting.
        /// </summary>
        public virtual DataConverter MessageDataConverter => JsonDataConverter.Default;

        /// <summary>
        /// The data converter used for the entity state.
        /// Implementations may override this setting.
        /// </summary>
        public virtual DataConverter StateDataConverter => this.MessageDataConverter;

        /// <summary>
        /// The data converter used for exceptions.
        /// Implementations may override this setting.
        /// </summary>
        public virtual DataConverter ErrorDataConverter => this.MessageDataConverter;

        /// <summary>
        /// If true, all effects of an entity operation (all state changes and all actions) are rolled back
        /// if the entity operation completes with an exception.
        /// Implementations may override this setting.
        /// </summary>
        public virtual bool RollbackOnExceptions => true;

        /// <summary>
        /// Options for executing entities.
        /// </summary>
        public EntityExecutionOptions EntityExecutionOptions { get; set; }

        /// <inheritdoc/>
        public override async Task<OperationBatchResult> ExecuteOperationBatchAsync(OperationBatchRequest operations, EntityExecutionOptions options)
        {
            this.EntityExecutionOptions = options;

            var result = new OperationBatchResult()
            {
                Results = new List<OperationResult>(),
                Actions = new List<OperationAction>(),
                EntityState = operations.EntityState,
            };

            var entityContext = new TaskEntityContext<TState>(
                this,
                EntityId.FromString(operations.InstanceId),
                options,
                operations,
                result);

            await entityContext.ExecuteBatchAsync();

            return result;
        }
    }
}