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
    using DurableTask.Core.Common;
    using DurableTask.Core.Entities.EventFormat;
    using DurableTask.Core.Entities.OperationFormat;
    using DurableTask.Core.Entities.StateFormat;
    using DurableTask.Core.Exceptions;
    using DurableTask.Core.History;
    using DurableTask.Core.Logging;
    using DurableTask.Core.Middleware;
    using DurableTask.Core.Tracing;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading.Tasks;

    internal class EntityWorkItemProcessor : TaskOrchestrationDispatcher.WorkItemProcessor
    {
        readonly LogHelper logHelper;
        readonly INameVersionObjectManager<TaskEntity> objectManager;
        readonly DispatchMiddlewarePipeline dispatchPipeline;
        readonly EntityBackendInformation entityBackendInformation;
        readonly string instanceId;
        readonly ErrorPropagationMode errorPropagationMode;

        SchedulerState schedulerState;
        int idCounter;

        public EntityWorkItemProcessor(
            TaskOrchestrationDispatcher dispatcher,
            TaskOrchestrationWorkItem workItem,
            LogHelper logHelper,
            INameVersionObjectManager<TaskEntity> objectManager,
            DispatchMiddlewarePipeline dispatchPipeline,
            EntityBackendInformation entityBackendInformation,
            ErrorPropagationMode errorPropagationMode) : base(dispatcher, workItem)
        {
            this.logHelper = logHelper;
            this.objectManager = objectManager;
            this.dispatchPipeline = dispatchPipeline;
            this.entityBackendInformation = entityBackendInformation;
            this.instanceId = workItem.InstanceId;
            this.errorPropagationMode = errorPropagationMode;
        }

        public override async Task ProcessWorkItemAsync()
        {
            // we start with processing all the requests and figuring out which ones to execute
            // results can depend on whether the entity is locked, what the maximum batch size is,
            // and whether the messages arrived out of order
            var workToDoNow = this.DetermineWork();

            if (workToDoNow.OperationCount > 0)
            {
                // execute the user-defined operations on this entity, via the middleware
                var result = await this.ExecuteViaMiddlewareAsync(workToDoNow);

                // go through all results
                // for each operation that is not a signal, send a result message back to the calling orchestrator
                for (int i = 0; i < result.Results.Count; i++)
                {
                    var req = workToDoNow.Operations[i];
                    if (!req.IsSignal)
                    {
                        this.SendResultMessage(req, result.Results[i]);
                    }
                }

                if (result.Results.Count < workToDoNow.OperationCount)
                {
                    // some operations were not processed
                    var deferred = workToDoNow.RemoveDeferredWork(result.Results.Count);
                    this.schedulerState.PutBack(deferred);
                    workToDoNow.ToBeContinued(this.schedulerState);
                }

                // update the entity state based on the result
                this.schedulerState.EntityState = result.EntityState;
                this.schedulerState.EntityExists = result.EntityState != null;

                // perform the actions
                foreach (var action in result.Actions)
                {
                    switch (action)
                    {
                        case (SendSignalOperationAction sendSignalAction):
                            this.SendSignalMessage(sendSignalAction);
                            break;
                        case (StartNewOrchestrationOperationAction startAction):
                            this.ProcessSendStartMessage(startAction);
                            break;
                    }
                }
            }

            // process the lock request, if any
            if (workToDoNow.LockRequest != null)
            {
                this.ProcessLockRequest(workToDoNow.LockRequest);
            }

            if (workToDoNow.ToBeRescheduled != null)
            {
                foreach (var request in workToDoNow.ToBeRescheduled)
                {
                    // Reschedule all signals that were received before their time
                    this.SendScheduledSelfMessage(request);
                }
            }

            if (workToDoNow.SuspendAndContinue)
            {
                this.SendContinueSelfMessage();
            }

            // this batch is complete. Since this is an entity, we now
            // (always) start a new execution, as in continue-as-new

            var serializedSchedulerState = this.SerializeSchedulerStateForNextExecution();
            var nextExecutionStartedEvent = new ExecutionStartedEvent(-1, serializedSchedulerState)
            {
                OrchestrationInstance = new OrchestrationInstance
                {
                    InstanceId = this.instanceId,
                    ExecutionId = Guid.NewGuid().ToString("N")
                },
                Tags = runtimeState.Tags,
                ParentInstance = runtimeState.ParentInstance,
                Name = runtimeState.Name,
                Version = runtimeState.Version
            };
            var entityStatus = new EntityStatus()
            {
                EntityExists = this.schedulerState.EntityExists,
                QueueSize = this.schedulerState.Queue?.Count ?? 0,
                LockedBy = this.schedulerState.LockedBy,
            };
            var serializedEntityStatus = JsonConvert.SerializeObject(entityStatus, Serializer.InternalSerializerSettings);

            // create the runtime state for the next execution
            this.runtimeState = new OrchestrationRuntimeState();
            this.runtimeState.Status = serializedEntityStatus;
            this.runtimeState.AddEvent(new OrchestratorStartedEvent(-1));
            this.runtimeState.AddEvent(nextExecutionStartedEvent);
            this.runtimeState.AddEvent(new OrchestratorCompletedEvent(-1));

            this.workItem.OrchestrationRuntimeState = this.runtimeState;
            this.instanceState = Utils.BuildOrchestrationState(this.runtimeState);
        }

        void ProcessLockRequest(RequestMessage request)
        {
            this.logHelper.EntityLockAcquired(this.instanceId, request);

            // mark the entity state as locked
            this.schedulerState.LockedBy = request.ParentInstanceId;

            request.Position++;

            if (request.Position < request.LockSet.Length)
            {
                // send lock request to next entity in the lock set
                var target = new OrchestrationInstance() { InstanceId = request.LockSet[request.Position].ToString() };
                this.SendLockRequestMessage(target, request);
            }
            else
            {
                // send lock acquisition completed response back to originating orchestration instance
                var target = new OrchestrationInstance() { InstanceId = request.ParentInstanceId, ExecutionId = request.ParentExecutionId };
                this.SendLockResponseMessage(target, request.Id);
            }
        }

        string SerializeSchedulerStateForNextExecution()
        {
            if (this.entityBackendInformation.SupportsImplicitEntityDeletion && this.schedulerState.IsEmpty && !this.schedulerState.Suspended)
            {
                // this entity scheduler is idle and the entity is deleted, so the instance and history can be removed from storage
                // we convey this to the durability provider by issuing a continue-as-new with null input
                return null;
            }
            else
            {
                // we persist the state of the entity scheduler and entity
                return JsonConvert.SerializeObject(this.schedulerState, typeof(SchedulerState), Serializer.InternalSerializerSettings);
            }
        }

        #region Preprocess to determine work

        Work DetermineWork()
        {
            Queue<RequestMessage> lockHolderMessages = null;
            Work batch = new Work();

            foreach (HistoryEvent e in this.runtimeState.Events)
            {
                switch (e.EventType)
                {
                    case EventType.ExecutionStarted:

                        this.schedulerState = new SchedulerState();

                        if (runtimeState.Input != null)
                        {
                            try
                            {
                                // restore the scheduler state from the input
                                JsonConvert.PopulateObject(runtimeState.Input, this.schedulerState, Serializer.InternalSerializerSettings);
                            }
                            catch (Exception exception)
                            {
                                throw new EntitySchedulerException("Failed to deserialize entity scheduler state - may be corrupted or wrong version.", exception);
                            }
                        }
                        break;

                    case EventType.EventRaised:
                        EventRaisedEvent eventRaisedEvent = (EventRaisedEvent)e;

                        if (EntityMessageEventNames.IsRequestMessage(eventRaisedEvent.Name))
                        {
                            // we are receiving an operation request or a lock request
                            var requestMessage = new RequestMessage();

                            try
                            {
                                JsonConvert.PopulateObject(eventRaisedEvent.Input, requestMessage, Serializer.InternalSerializerSettings);
                            }
                            catch (Exception exception)
                            {
                                throw new EntitySchedulerException("Failed to deserialize incoming request message - may be corrupted or wrong version.", exception);
                            }

                            IEnumerable<RequestMessage> deliverNow;

                            if (requestMessage.ScheduledTime.HasValue)
                            {
                                if ((requestMessage.ScheduledTime.Value - DateTime.UtcNow) > TimeSpan.FromMilliseconds(100))
                                {
                                    // message was delivered too early. This can happen e.g. if the orchestration service has limits on the delay times for messages.
                                    // We handle this by rescheduling the message instead of processing it.
                                    deliverNow = Array.Empty<RequestMessage>();
                                    batch.AddMessageToBeRescheduled(requestMessage);
                                }
                                else
                                {
                                    // the message is scheduled to be delivered immediately.
                                    // There are no FIFO guarantees for scheduled messages, so we skip the message sorter.
                                    deliverNow = new RequestMessage[] { requestMessage };
                                }
                            }
                            else
                            {
                                // run this through the message sorter to help with reordering and duplicate filtering
                                deliverNow = this.schedulerState.MessageSorter.ReceiveInOrder(requestMessage, this.entityBackendInformation.EntityMessageReorderWindow);
                            }

                            foreach (var message in deliverNow)
                            {
                                if (this.schedulerState.LockedBy != null && this.schedulerState.LockedBy == message.ParentInstanceId)
                                {
                                    if (lockHolderMessages == null)
                                    {
                                        lockHolderMessages = new Queue<RequestMessage>();
                                    }

                                    lockHolderMessages.Enqueue(message);
                                }
                                else
                                {
                                    this.schedulerState.Enqueue(message);
                                }
                            }
                        }
                        else if (EntityMessageEventNames.IsReleaseMessage(eventRaisedEvent.Name))
                        {
                            // we are receiving a lock release
                            var message = new ReleaseMessage();
                            try
                            {
                                // restore the scheduler state from the input
                                JsonConvert.PopulateObject(eventRaisedEvent.Input, message, Serializer.InternalSerializerSettings);
                            }
                            catch (Exception exception)
                            {
                                throw new EntitySchedulerException("Failed to deserialize lock release message - may be corrupted or wrong version.", exception);
                            }

                            if (this.schedulerState.LockedBy == message.ParentInstanceId)
                            {
                                this.logHelper.EntityLockReleased(this.instanceId, message);
                                this.schedulerState.LockedBy = null;
                            }
                        }
                        else
                        {
                            // this is a continue message.
                            // Resumes processing of previously queued operations, if any.
                            this.schedulerState.Suspended = false;
                        }

                        break;
                }
            }

            // lock holder messages go to the front of the queue
            if (lockHolderMessages != null)
            {
                this.schedulerState.PutBack(lockHolderMessages);
            }

            if (!this.schedulerState.Suspended)
            {
                // 2. We add as many requests from the queue to the batch as possible,
                // stopping at lock requests or when the maximum batch size is reached
                while (this.schedulerState.MayDequeue())
                {
                    if (batch.OperationCount == this.entityBackendInformation.MaxEntityOperationBatchSize)
                    {
                        // we have reached the maximum batch size already
                        // insert a delay after this batch to ensure write back
                        batch.ToBeContinued(this.schedulerState);
                        break;
                    }

                    var request = this.schedulerState.Dequeue();

                    if (request.IsLockRequest)
                    {
                        batch.AddLockRequest(request);
                        break;
                    }
                    else
                    {
                        batch.AddOperation(request);
                    }
                }
            }

            return batch;
        }

        class Work
        {
            List<RequestMessage> operationBatch; // a (possibly empty) sequence of operations to be executed on the entity
            RequestMessage lockRequest = null; // zero or one lock request to be executed after all the operations
            List<RequestMessage> toBeRescheduled; // a (possibly empty) list of timed messages that were delivered too early and should be rescheduled
            bool suspendAndContinue; // a flag telling as to send ourselves a continue signal

            public int OperationCount => this.operationBatch?.Count ?? 0;
            public IReadOnlyList<RequestMessage> Operations => this.operationBatch;
            public IReadOnlyList<RequestMessage> ToBeRescheduled => this.toBeRescheduled;
            public RequestMessage LockRequest => this.lockRequest;
            public bool SuspendAndContinue => this.suspendAndContinue;

            public void AddOperation(RequestMessage operationMessage)
            {
                if (this.operationBatch == null)
                {
                    this.operationBatch = new List<RequestMessage>();
                }
                this.operationBatch.Add(operationMessage);
            }

            public void AddLockRequest(RequestMessage lockRequest)
            {
                Debug.Assert(this.lockRequest == null);
                this.lockRequest = lockRequest;
            }

            public void AddMessageToBeRescheduled(RequestMessage requestMessage)
            {
                if (this.toBeRescheduled == null)
                {
                    this.toBeRescheduled = new List<RequestMessage>();
                }
                this.toBeRescheduled.Add(requestMessage);
            }

            public void ToBeContinued(SchedulerState schedulerState)
            {
                if (!schedulerState.Suspended)
                {
                    this.suspendAndContinue = true;
                }
            }

            public List<OperationRequest> GetOperationRequests()
            {
                var operations = new List<OperationRequest>(this.operationBatch.Count);
                for (int i = 0; i < this.operationBatch.Count; i++)
                {
                    var request = this.operationBatch[i];
                    operations.Add(new OperationRequest()
                    {
                        Operation = request.Operation,
                        Id = request.Id,
                        Input = request.Input,
                    });
                }
                return operations;
            }

            public Queue<RequestMessage> RemoveDeferredWork(int index)
            {
                var deferred = new Queue<RequestMessage>();
                for (int i = index; i < this.operationBatch.Count; i++)
                {
                    deferred.Enqueue(this.operationBatch[i]);
                }
                this.operationBatch.RemoveRange(index, this.operationBatch.Count - index);
                if (this.lockRequest != null)
                {
                    deferred.Enqueue(this.lockRequest);
                    this.lockRequest = null;
                }
                return deferred;
            }
        }

        #endregion

        #region Send Messages

        void SendResultMessage(RequestMessage request, OperationResult result)
        {
            var destination = new OrchestrationInstance()
            {
                InstanceId = request.ParentInstanceId,
                ExecutionId = request.ParentExecutionId,
            };
            var responseMessage = new ResponseMessage()
            {
                Result = result.Result,
                ErrorMessage = result.ErrorMessage,
                FailureDetails = result.FailureDetails,
            };
            this.ProcessSendEventMessage(destination, EntityMessageEventNames.ResponseMessageEventName(request.Id), responseMessage);
        }

        void SendSignalMessage(SendSignalOperationAction action)
        {
            OrchestrationInstance destination = new OrchestrationInstance()
            {
                InstanceId = action.InstanceId
            };
            RequestMessage message = new RequestMessage()
            {
                ParentInstanceId = this.instanceId,
                ParentExecutionId = null, // for entities, message sorter persists across executions
                Id = Guid.NewGuid(),
                IsSignal = true,
                Operation = action.Name,
                ScheduledTime = action.ScheduledTime,
            };
            string eventName;
            if (action.ScheduledTime.HasValue)
            {
                (DateTime original, DateTime capped) = this.entityBackendInformation.GetCappedScheduledTime(DateTime.UtcNow, action.ScheduledTime.Value);
                eventName = EntityMessageEventNames.ScheduledRequestMessageEventName(capped);
            }
            else
            {
                eventName = EntityMessageEventNames.RequestMessageEventName;
                this.schedulerState.MessageSorter.LabelOutgoingMessage(message, action.InstanceId, DateTime.UtcNow, this.entityBackendInformation.EntityMessageReorderWindow);
            }
            this.ProcessSendEventMessage(destination, eventName, message);
        }

        internal void SendLockRequestMessage(OrchestrationInstance target, RequestMessage message)
        {
            this.schedulerState.MessageSorter.LabelOutgoingMessage(message, target.InstanceId, DateTime.UtcNow, this.entityBackendInformation.EntityMessageReorderWindow);
            this.ProcessSendEventMessage(target, EntityMessageEventNames.RequestMessageEventName, message);
        }

        internal void SendLockResponseMessage(OrchestrationInstance target, Guid requestId)
        {
            var message = new ResponseMessage()
            {
                Result = "Lock Acquisition Completed", // ignored by receiver but shows up in traces
            };
            this.ProcessSendEventMessage(target, EntityMessageEventNames.ResponseMessageEventName(requestId), message);
        }

        void SendScheduledSelfMessage(RequestMessage request)
        {
            var self = new OrchestrationInstance()
            {
                InstanceId = this.instanceId,
            };
            this.ProcessSendEventMessage(self, EntityMessageEventNames.ScheduledRequestMessageEventName(request.ScheduledTime.Value), request);
        }

        void SendContinueSelfMessage()
        {
            var self = new OrchestrationInstance()
            {
                InstanceId = this.instanceId,
            };
            this.ProcessSendEventMessage(self, EntityMessageEventNames.ContinueMessageEventName, null);
        }

        void ProcessSendEventMessage(OrchestrationInstance destination, string eventName, object eventContent)
        {
            string serializedContent = null;
            if (eventContent != null)
            {
                serializedContent = JsonConvert.SerializeObject(eventContent, Serializer.InternalSerializerSettings);
            }
            
            var eventSentEvent = new EventSentEvent(this.idCounter++)
            {
                InstanceId = destination.InstanceId,     
                Name = eventName,
                Input = serializedContent,
            };
            this.logHelper.RaisingEvent(runtimeState.OrchestrationInstance!, eventSentEvent);
            
            this.orchestratorMessages.Add(new TaskMessage
            {
                OrchestrationInstance = destination,
                Event = new EventRaisedEvent(-1, serializedContent)
                {
                    Name = eventName,
                    Input = serializedContent,
                },
            });
        }

        void ProcessSendStartMessage(StartNewOrchestrationOperationAction action)
        {
            OrchestrationInstance destination = new OrchestrationInstance()
            {
                InstanceId = action.InstanceId,
                ExecutionId = Guid.NewGuid().ToString("N"),
            };
            var executionStartedEvent = new ExecutionStartedEvent(-1, action.Input)
            {
                Tags = OrchestrationTags.MergeTags(action.Tags, runtimeState.Tags),
                OrchestrationInstance = destination,
                ParentInstance = new ParentInstance
                {
                    OrchestrationInstance = runtimeState.OrchestrationInstance,
                    Name = runtimeState.Name,
                    Version = runtimeState.Version,
                    TaskScheduleId = idCounter++,
                },
                Name = action.Name,
                Version = action.Version,
            };
            this.logHelper.SchedulingOrchestration(executionStartedEvent);

            this.orchestratorMessages.Add(new TaskMessage
            {
                OrchestrationInstance = destination,
                Event = executionStartedEvent,
            });
        }

        #endregion

        async Task<OperationBatchResult> ExecuteViaMiddlewareAsync(Work workToDoNow)
        {
            // the request object that will be passed to the worker
            var request = new OperationBatchRequest()
            {
                InstanceId = this.instanceId,
                EntityState = this.schedulerState.EntityState,
                Operations = workToDoNow.GetOperationRequests(),
            };

            this.logHelper.EntityBatchExecuting(request);

            string entityName = EntityId.FromString(this.instanceId).EntityName;
            string entityVersion = string.Empty; // TODO consider whether we should support explicit versions
 
            // Get the TaskOrchestration implementation. If it's not found, it either means that the developer never
            // registered it (which is an error, and we'll throw for this further down) or it could be that some custom
            // middleware (e.g. out-of-process execution middleware) is intended to implement the orchestration logic.
            TaskEntity taskEntity = this.objectManager.GetObject(entityName, entityVersion);

            var dispatchContext = new DispatchMiddlewareContext();
            dispatchContext.SetProperty(request);

            await this.dispatchPipeline.RunAsync(dispatchContext, async _ =>
            {
                // Check to see if the custom middleware intercepted and substituted the orchestration execution
                // with its own execution behavior, providing us with the end results. If so, we can terminate
                // the dispatch pipeline here.
                var resultFromMiddleware = dispatchContext.GetProperty<OperationBatchResult>();
                if (resultFromMiddleware != null)
                {
                    return;
                }

                if (taskEntity == null)
                {
                    throw TraceHelper.TraceExceptionInstance(
                        TraceEventType.Error,
                        "TaskOrchestrationDispatcher-EntityTypeMissing",
                        runtimeState.OrchestrationInstance,
                        new TypeMissingException($"Entity not found: {entityName}"));
                }

                var options = new EntityExecutionOptions()
                {
                    EntityBackendInformation = this.entityBackendInformation,
                    ErrorPropagationMode = this.errorPropagationMode,
                };

                var resultFromTaskEntityObject = await taskEntity.ExecuteOperationBatchAsync(request, options);

                dispatchContext.SetProperty(resultFromTaskEntityObject);
            });

            var result = dispatchContext.GetProperty<OperationBatchResult>();

            this.logHelper.EntityBatchExecuted(request, result);

            return result;
        }
    }
}
