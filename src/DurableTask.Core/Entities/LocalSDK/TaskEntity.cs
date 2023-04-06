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
    /// TaskEntity representing an entity with typed state.
    /// </summary>
    /// <typeparam name="TState">The type of the entity state.</typeparam>
    public abstract class TaskEntity<TState> : TaskEntity
    {
        /// <summary>
        /// Defines how this entity processes operations. Implementations must override this.
        /// </summary>
        /// <param name="context">The context for the operation.</param>
        /// <returns>A task that completes with the return value of the operation.</returns>
        public abstract ValueTask<object> ExecuteOperationAsync(EntityContext<TState> context);

        /// <summary>
        /// A function for creating the initial state of the entity.
        /// Implementations may override this if they want to perform a different initialization.
        /// </summary>
        /// <remarks>This is only called when the entity state is created, not when it is reloaded from storage.
        /// When loading the entity from storage, the entity state is created using a DataConverter.</remarks>
        public virtual TState CreateInitialState(EntityContext<TState> context)
        {
            return default;
        }

        /// <summary>
        /// We cache the entity context inside the TaskEntity object. That way, it is possible for applications
        /// to use a caching implementation of <see cref="INameVersionObjectManager{TaskEntity}"/> if they want
        /// to avoid constructing a fresh execution context, and allow deserialized state objects to be reused.
        /// </summary>
        internal TaskEntityContext<TState> CachedContext;

        internal override Task<OperationBatchResult> ExecuteOperationBatchAsync(OperationBatchRequest operations, EntityExecutionOptions options)
        {
            this.CachedContext ??= new TaskEntityContext<TState>(this, options);
            return this.CachedContext.ExecuteBatchAsync(operations);
        }
    }
}