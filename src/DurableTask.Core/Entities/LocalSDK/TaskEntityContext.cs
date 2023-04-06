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
    using DurableTask.Core.Common;
    using DurableTask.Core.Entities;
    using DurableTask.Core.Entities.OperationFormat;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.Reflection;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;

    internal class TaskEntityContext<TState> : EntityContext<TState>
    {
        readonly TaskEntity<TState> taskEntity;
        readonly EntityExecutionOptions executionOptions;

        EntityId entityId;
        int batchPosition = -1;
        int batchSize;
        string lastSerializedState;
        OperationRequest currentOperationRequest;
        OperationResult currentOperationResult;
        StateAccess currentStateAccess;
        TState currentState;
        List<OperationAction> actions;

        public TaskEntityContext(TaskEntity<TState> taskEntity, EntityExecutionOptions options)
        {
            this.taskEntity = taskEntity;
            this.executionOptions = options;
        }

        public override EntityId EntityId => this.entityId;

        public override EntityExecutionOptions EntityExecutionOptions => this.executionOptions;

        public override string OperationName => this.currentOperationRequest.Operation;
        public override int BatchSize => this.batchSize;
        public override int BatchPosition => this.batchPosition;

        OperationRequest CurrentOperation => this.currentOperationRequest;

        // The last serialized checkpoint of the entity state is always stored in
        // this.LastSerializedState. The current state is determined by this.CurrentStateAccess and this.CurrentState.
        internal enum StateAccess
        {
            NotAccessed, // current state is stored in this.LastSerializedState
            Accessed, // current state is stored in this.currentState 
            Clean, // current state is stored in both this.currentState (deserialized) and in this.LastSerializedState
            Deleted, // current state is deleted
        }

        public override bool HasState
        {
            get
            {
                switch (this.currentStateAccess)
                {
                    case StateAccess.Accessed:
                    case StateAccess.Clean:
                        return true;

                    case StateAccess.Deleted:
                        return false;

                    default: return this.lastSerializedState != null;
                }
            }
        }

        public override TState State
        {
            get
            {
                if (this.currentStateAccess == StateAccess.Accessed)
                {
                    return this.currentState;
                }
                else if (this.currentStateAccess == StateAccess.Clean)
                {
                    this.currentStateAccess = StateAccess.Accessed;
                    return this.currentState;
                }

                TState result;

                if (this.lastSerializedState != null && this.currentStateAccess != StateAccess.Deleted)
                {
                    try
                    {
                        result = this.executionOptions.StateDataConverter.Deserialize<TState>(this.lastSerializedState);
                    }
                    catch (Exception e)
                    {
                        throw new EntitySchedulerException($"Failed to deserialize entity state: {e.Message}", e);
                    }
                }
                else
                {
                    try
                    {
                        result = this.taskEntity.CreateInitialState(this);
                    }
                    catch (Exception e)
                    {
                        throw new EntitySchedulerException($"Failed to initialize entity state: {e.Message}", e);
                    }
                }

                this.currentStateAccess = StateAccess.Accessed;
                this.currentState = result;
                return result;
            }
            set
            {
                this.currentState = value;
                this.currentStateAccess = StateAccess.Accessed;
            }
        }

        public override void DeleteState()
        {
            this.currentStateAccess = StateAccess.Deleted;
            this.currentState = default;
        }

        public void Rollback(int positionBeforeCurrentOperation)
        {
            // We discard the current state, which means we go back to the last serialized one
            this.currentStateAccess = StateAccess.NotAccessed;
            this.currentState = default;

            // we also roll back the list of outgoing messages,
            // so any signals sent by this operation are discarded.
            this.actions.RemoveRange(positionBeforeCurrentOperation, this.actions.Count - positionBeforeCurrentOperation);
        }

        private bool TryWriteback(out OperationResult serializationErrorResult, OperationRequest operationRequest = null)
        {
            if (this.currentStateAccess == StateAccess.Deleted)
            {
                this.lastSerializedState = null;
                this.currentStateAccess = StateAccess.NotAccessed;
            }
            else if (this.currentStateAccess == StateAccess.Accessed)
            {
                try
                {
                    string serializedState = this.executionOptions.StateDataConverter.Serialize(this.currentState);
                    this.lastSerializedState = serializedState;
                    this.currentStateAccess = StateAccess.Clean;
                }
                catch (Exception serializationException) when (!Utils.IsFatal(serializationException))
                {
                    // we cannot serialize the entity state - this is an application error. To help users diagnose this, 
                    // we wrap it into a descriptive exception, and propagate this error result to any calling orchestrations.

                    serializationException = new EntitySchedulerException(
                        $"Operation was rolled back because state for entity '{this.EntityId}' could not be serialized: {serializationException.Message}", serializationException);
                    serializationErrorResult = new OperationResult();
                    this.CaptureExceptionInOperationResult(serializationErrorResult, serializationException);

                    // we have no choice but to roll back to the state prior to this operation
                    this.currentStateAccess = StateAccess.NotAccessed;
                    this.currentState = default;

                    return false;
                }
            }
            else
            {
                // the state was not accessed, or is clean, so we don't need to write anything back
            }

            serializationErrorResult = null;
            return true;
        }

        public override object GetInput(Type inputType)
        {
            try
            {
                return this.executionOptions.MessageDataConverter.Deserialize(this.CurrentOperation.Input, inputType);
            }
            catch (Exception e)
            {
                throw new EntitySchedulerException($"Failed to deserialize input for operation '{this.CurrentOperation.Operation}': {e.Message}", e);
            }
        }


        public override void SignalEntity(EntityId entity, string operationName, object operationInput = null)
        {
            this.SignalEntityInternal(entity, null, operationName, operationInput);
        }


        public override void SignalEntity(EntityId entity, DateTime scheduledTimeUtc, string operationName, object operationInput = null)
        {
            this.SignalEntityInternal(entity, scheduledTimeUtc, operationName, operationInput);
        }

        private void SignalEntityInternal(EntityId entity, DateTime? scheduledTimeUtc, string operationName, object operationInput)
        {
            if (operationName == null)
            {
                throw new ArgumentNullException(nameof(operationName));
            }

            string functionName = entity.Name;

            var action = new SendSignalOperationAction()
            {
                InstanceId = entity.ToString(),
                Name = operationName,
                ScheduledTime = scheduledTimeUtc,
                Input = null,
            };

            if (operationInput != null)
            {
                try
                {
                    action.Input = this.executionOptions.MessageDataConverter.Serialize(operationInput);
                }
                catch (Exception e)
                {
                    throw new EntitySchedulerException($"Failed to serialize input for operation '{operationName}': {e.Message}", e);
                }
            }

            // add the action to the results, under a lock since user code may be concurrent
            lock (this.actions)
            {
                this.actions.Add(action);
            }
        }

        public override string StartNewOrchestration(string name, string version, object input, string instanceId = null)
        {
            if (string.IsNullOrEmpty(instanceId))
            {
                instanceId = Guid.NewGuid().ToString(); // this is an entity, so we don't need to be deterministic
            }
            else if (DurableTask.Core.Common.Entities.IsEntityInstance(instanceId))
            {
                throw new ArgumentException(nameof(instanceId), "Invalid orchestration instance ID, must not be an entity ID");
            }

            var action = new StartNewOrchestrationOperationAction()
            {
                InstanceId = instanceId,
                Name = name,
                Version = version,
                Tags = new Dictionary<string, string>() { { OrchestrationTags.FireAndForget, "" } },
            };

            if (input != null)
            {
                try
                {
                    action.Input = this.executionOptions.MessageDataConverter.Serialize(input);
                }
                catch (Exception e)
                {
                    throw new EntitySchedulerException($"Failed to serialize input for orchestration '{name}': {e.Message}", e);
                }
            }

            // add the action to the results, under a lock since user code may be concurrent
            lock (this.actions)
            {
                this.actions.Add(action);
            }

            return instanceId;
        }

        public async Task<OperationBatchResult> ExecuteBatchAsync(OperationBatchRequest batchRequest)
        {
            var entityId = EntityId.FromString(batchRequest.InstanceId);

            if (entityId.Equals(this.entityId) && batchRequest.EntityState == this.lastSerializedState)
            {
                // we were called before, with the same entityId, and the same state.
                // We can therefore keep the current state as is.
            }
            else
            {
                this.entityId = entityId;
                this.lastSerializedState = batchRequest.EntityState;
                this.currentState = default;
                this.currentStateAccess = StateAccess.NotAccessed;
            }

            this.batchSize = batchRequest.Operations.Count;
            var actions = this.actions = new List<OperationAction>();
            var results = new List<OperationResult>();

            // execute all the operations in a loop and record the results.
            for (int i = 0; i < batchRequest.Operations.Count; i++)
            {
                this.batchPosition = i;
                this.currentOperationRequest = batchRequest.Operations[i];
                this.currentOperationResult = new OperationResult();

                // process the operation (handling any errors if necessary)
                await this.ProcessOperationRequestAsync();

                results.Add(this.currentOperationResult);
            }

            if (this.executionOptions.RollbackOnExceptions)
            {
                // the state has already been written back, since it is
                // done right after each operation.
            }
            else
            {
                // we are writing back the state only now, after the whole batch is complete.

                var writeBackSuccessful = this.TryWriteback(out OperationResult serializationErrorMessage);

                if (!writeBackSuccessful)
                {
                    // failed to write state, we now consider all operations in the batch as failed.
                    // We thus record the results (which are now fail results) and commit the batch
                    // with the original state.

                    // we clear the actions (if we cannot update the state, we should not take other actions either)
                    actions.Clear();

                    // we replace all response messages with the serialization error message,
                    // so that callers get to know that this operation failed, and why
                    for (int i = 0; i < results.Count; i++)
                    {
                        results[i] = serializationErrorMessage;
                    }
                }
            }

            // clear these fields before returning, so if this context is being cached somewhere, it keeps only the relevant information.
            this.batchPosition = -1;
            this.batchSize = 0;
            this.currentOperationRequest = null;
            this.currentOperationResult = null;
            this.actions = null;

            return new OperationBatchResult()
            {
                Results = results,
                Actions = actions,
                EntityState = this.lastSerializedState,
            };
        }

        async ValueTask ProcessOperationRequestAsync()
        {
            var actionPositionCheckpoint = this.actions.Count;

            try
            {
                object returnedResult = await taskEntity.ExecuteOperationAsync(this);

                if (returnedResult != null) 
                {
                    try
                    {
                        this.currentOperationResult.Result = this.executionOptions.MessageDataConverter.Serialize(returnedResult);
                    }
                    catch (Exception e)
                    {
                        throw new EntitySchedulerException($"Failed to serialize output for operation '{this.CurrentOperation.Operation}': {e.Message}", e);
                    }
                }
            }
            catch (Exception e) when (!Utils.IsFatal(e) && !Utils.IsExecutionAborting(e))
            {
                this.CaptureExceptionInOperationResult(this.currentOperationResult, e);
            }

            if (this.executionOptions.RollbackOnExceptions)
            {
                // we write back the entity state after each successful operation
                if (this.currentOperationResult.ErrorMessage == null)
                {
                    if (!this.TryWriteback(out OperationResult errorResult, this.currentOperationRequest))
                    {
                        // state serialization failed; create error response and roll back.
                        this.currentOperationResult = errorResult;
                    }
                }

                if (this.currentOperationResult.ErrorMessage != null)
                {
                    // we must also roll back any actions that this operation has issued
                    this.Rollback(actionPositionCheckpoint);
                }
            }
        }

        public void CaptureExceptionInOperationResult(OperationResult result, Exception originalException)
        {
            // a non-null ErrorMessage field is how we track internally that this result represents a failed operation
            result.ErrorMessage = originalException.Message ?? originalException.GetType().FullName;

            // record additional information, based on the error propagation mode
            switch (this.executionOptions.ErrorPropagationMode)
            {
                case ErrorPropagationMode.SerializeExceptions:
                    try
                    {
                        result.Result = this.executionOptions.ErrorDataConverter.Serialize(originalException);
                    }
                    catch (Exception serializationException) when (!Utils.IsFatal(serializationException))
                    {
                        // we can't serialize the original exception. We can't throw it here.
                        // So let us try to at least serialize the serialization exception
                        // because this information may help users that are trying to troubleshoot their application.
                        try
                        {
                            result.Result = this.executionOptions.ErrorDataConverter.Serialize(serializationException);
                        }
                        catch (Exception serializationExceptionSerializationException) when (!Utils.IsFatal(serializationExceptionSerializationException))
                        {
                            // there seems to be nothing we can do.
                        }
                    }
                    break;

                case ErrorPropagationMode.UseFailureDetails:
                    result.FailureDetails = new FailureDetails(originalException);
                    break;
            }
        }
    }
}