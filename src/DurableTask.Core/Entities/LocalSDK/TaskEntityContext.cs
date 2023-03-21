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
        readonly OperationBatchRequest batchRequest;
        readonly OperationBatchResult batchResult;

        int batchPosition;
        OperationResult currentOperationResult;
        StateAccess currentStateAccess;
        TState currentState;

        public TaskEntityContext(TaskEntity<TState> taskEntity, EntityId entityId, EntityExecutionOptions options, OperationBatchRequest batchRequest, OperationBatchResult batchResult)
        {
            this.taskEntity = taskEntity;
            this.EntityId = entityId;
            this.executionOptions = options;
            this.batchRequest = batchRequest;
            this.batchResult = batchResult;
        }

        public override string EntityName => this.EntityId.EntityName;
        public override string EntityKey => this.EntityId.EntityKey;
        public override EntityId EntityId { get; }

        public EntityExecutionOptions ExecutionOptions => this.executionOptions;

        public override string OperationName => this.batchRequest.Operations[this.batchPosition].Operation;
        public override int BatchSize => this.batchRequest.Operations.Count;
        public override int BatchPosition => this.batchPosition;

        OperationRequest CurrentOperation => this.batchRequest.Operations[batchPosition];

        // The last serialized checkpoint of the entity state is always stored in
        // this.LastSerializedState. The current state is determined by this.CurrentStateAccess and this.CurrentState.
        internal enum StateAccess
        {
            NotAccessed, // current state is stored in this.LastSerializedState
            Accessed, // current state is stored in this.currentState 
            Clean, // current state is stored in both this.currentState (deserialized) and in this.LastSerializedState
            Deleted, // current state is deleted
        }

        internal string LastSerializedState
        {
            get { return this.batchResult.EntityState; }
            set { this.batchResult.EntityState = value; }
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

                    default: return this.LastSerializedState != null;
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

                if (this.LastSerializedState != null && this.currentStateAccess != StateAccess.Deleted)
                {
                    try
                    {
                        result = taskEntity.StateDataConverter.Deserialize<TState>(this.LastSerializedState);
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
            this.batchResult.Actions.RemoveRange(positionBeforeCurrentOperation, this.batchResult.Actions.Count - positionBeforeCurrentOperation);
        }

        private bool TryWriteback(out OperationResult serializationErrorResult, OperationRequest operationRequest = null)
        {
            if (this.currentStateAccess == StateAccess.Deleted)
            {
                this.LastSerializedState = null;
                this.currentStateAccess = StateAccess.NotAccessed;
            }
            else if (this.currentStateAccess == StateAccess.Accessed)
            {
                try
                {
                    string serializedState = this.taskEntity.StateDataConverter.Serialize(this.currentState);
                    this.LastSerializedState = serializedState;
                    this.currentStateAccess = StateAccess.Clean;          
                }
                catch (Exception serializationException)
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

        public override TInput GetInput<TInput>()
        {
            try
            {
                return this.taskEntity.MessageDataConverter.Deserialize<TInput>(this.CurrentOperation.Input);
            }
            catch(Exception e)
            {
                throw new EntitySchedulerException($"Failed to deserialize input for operation '{this.CurrentOperation.Operation}': {e.Message}", e);
            }
        }

        public override object GetInput(Type inputType)
        {
            try
            {
                return this.taskEntity.MessageDataConverter.Deserialize(this.CurrentOperation.Input, inputType);
            }
            catch (Exception e)
            {
                throw new EntitySchedulerException($"Failed to deserialize input for operation '{this.CurrentOperation.Operation}': {e.Message}", e);
            }
        }

        public override void Return(object result)
        {
            try
            {
                this.currentOperationResult.Result = this.taskEntity.MessageDataConverter.Serialize(result);
            }
            catch (Exception e)
            {
                throw new EntitySchedulerException($"Failed to serialize output for operation '{this.CurrentOperation.Operation}': {e.Message}", e);
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

            string functionName = entity.EntityName;

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
                    action.Input = taskEntity.MessageDataConverter.Serialize(operationInput);
                }
                catch (Exception e)
                {
                    throw new EntitySchedulerException($"Failed to serialize input for operation '{operationName}': {e.Message}", e);
                }
            }

            // add the action to the results, under a lock since user code may be concurrent
            lock (this.batchResult.Actions)
            {
                this.batchResult.Actions.Add(action);
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
                    action.Input = taskEntity.MessageDataConverter.Serialize(input);
                }
                catch (Exception e)
                {
                    throw new EntitySchedulerException($"Failed to serialize input for orchestration '{name}': {e.Message}", e);
                }
            }

            // add the action to the results, under a lock since user code may be concurrent
            lock (this.batchResult.Actions)
            {
                this.batchResult.Actions.Add(action);
            }

            return instanceId;
        }

        public async Task ExecuteBatchAsync()
        {
            // execute all the operations in a loop and record the results.
            for (int i = 0; i < this.batchRequest.Operations.Count; i++)
            {
                await this.ProcessOperationRequestAsync(i);
            }

            if (this.taskEntity.RollbackOnExceptions)
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
                    this.batchResult.Actions.Clear();

                    // we replace all response messages with the serialization error message,
                    // so that callers get to know that this operation failed, and why
                    for (int i = 0; i < this.batchResult.Results.Count; i++)
                    {
                        this.batchResult.Results[i] = serializationErrorMessage;
                    }
                }
            }
        }

        async ValueTask ProcessOperationRequestAsync(int index)
        {
            // set context for operation
            var operation = this.batchRequest.Operations[index];
            this.batchPosition = index;
            this.currentOperationResult = new OperationResult();

            var actionPositionCheckpoint = this.batchResult.Actions.Count;

            try
            {
                await taskEntity.ExecuteOperationAsync(this);
            }
            catch (Exception e) when (!Utils.IsFatal(e) && !Utils.IsExecutionAborting(e))
            {
                this.CaptureExceptionInOperationResult(this.currentOperationResult, e);
            }

            if (this.taskEntity.RollbackOnExceptions)
            {
                // we write back the entity state after each successful operation
                if (this.currentOperationResult.ErrorMessage == null)
                {
                    if (!this.TryWriteback(out OperationResult errorResult, operation))
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
         
            // write the result to the list of results for the batch
            this.batchResult.Results.Add(this.currentOperationResult);
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
                        result.Result = this.taskEntity.ErrorDataConverter.Serialize(originalException);
                    }
                    catch (Exception serializationException) when (!Utils.IsFatal(serializationException))
                    {
                        // we can't serialize the original exception. We can't throw it here.
                        // So let us try to at least serialize the serialization exception
                        // because this information may help users that are trying to troubleshoot their application.
                        try
                        {
                            result.Result = this.taskEntity.ErrorDataConverter.Serialize(serializationException);
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