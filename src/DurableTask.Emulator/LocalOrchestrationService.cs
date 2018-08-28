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

namespace DurableTask.Emulator
{
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Fully functional in-proc orchestration service for testing
    /// </summary>
    public class LocalOrchestrationService : IOrchestrationService, IOrchestrationServiceClient, IDisposable
    {
        Dictionary<string, byte[]> sessionState;
        List<TaskMessage> timerMessages;

        int MaxConcurrentWorkItems = 20;

        // dictionary<instanceid, dictionary<executionid, orchestrationstate>>
        //Dictionary<string, Dictionary<string, OrchestrationState>> instanceStore;

        PeekLockSessionQueue orchestratorQueue;
        PeeklockQueue workerQueue;

        CancellationTokenSource cancellationTokenSource;

        Dictionary<string, Dictionary<string, OrchestrationState>> instanceStore;

        //Dictionary<string, Tuple<List<TaskMessage>, byte[]>> sessionLock;

        object thisLock = new object();
        object timerLock = new object();

        ConcurrentDictionary<string, TaskCompletionSource<OrchestrationState>> orchestrationWaiters;

        /// <summary>
        ///     Creates a new instance of the LocalOrchestrationService with default settings
        /// </summary>
        public LocalOrchestrationService()
        {
            this.orchestratorQueue = new PeekLockSessionQueue();
            this.workerQueue = new PeeklockQueue();

            this.sessionState = new Dictionary<string, byte[]>();

            this.timerMessages = new List<TaskMessage>();
            this.instanceStore = new Dictionary<string, Dictionary<string, OrchestrationState>>();
            this.orchestrationWaiters = new ConcurrentDictionary<string, TaskCompletionSource<OrchestrationState>>();
            this.cancellationTokenSource = new CancellationTokenSource();
        }

        async Task TimerMessageSchedulerAsync()
        {
            while (!this.cancellationTokenSource.Token.IsCancellationRequested)
            {
                lock (this.timerLock)
                {
                    foreach (TaskMessage tm in this.timerMessages.ToList())
                    {
                        TimerFiredEvent te = tm.Event as TimerFiredEvent;

                        if (te == null)
                        {
                            // AFFANDAR : TODO : unobserved task exception
                            throw new InvalidOperationException("Invalid timer message");
                        }

                        if (te.FireAt <= DateTime.UtcNow)
                        {
                            this.orchestratorQueue.SendMessage(tm);
                            this.timerMessages.Remove(tm);
                        }
                    }
                 }

                await Task.Delay(TimeSpan.FromSeconds(1));
            }

            return;
        }

        /******************************/
        // management methods
        /******************************/
        /// <inheritdoc />
        public Task CreateAsync()
        {
            return CreateAsync(true);
        }

        /// <inheritdoc />
        public Task CreateAsync(bool recreateInstanceStore)
        {
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public Task CreateIfNotExistsAsync()
        {
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public Task DeleteAsync()
        {
            return DeleteAsync(true);
        }

        /// <inheritdoc />
        public Task DeleteAsync(bool deleteInstanceStore)
        {
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public Task StartAsync()
        {
            Task.Run(() => TimerMessageSchedulerAsync());
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public Task StopAsync(bool isForced)
        {
            this.cancellationTokenSource.Cancel();
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public Task StopAsync()
        {
            return StopAsync(false);
        }

        /// <inheritdoc />
        public bool IsTransientException(Exception exception)
        {
            return false;
        }

        /******************************/
        // client methods
        /******************************/
        /// <inheritdoc />
        public Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            ExecutionStartedEvent ee = creationMessage.Event as ExecutionStartedEvent;

            if (ee == null)
            {
                throw new InvalidOperationException("Invalid creation task message");
            }

            lock (this.thisLock)
            {
                this.orchestratorQueue.SendMessage(creationMessage);

                Dictionary<string, OrchestrationState> ed;

                if (!this.instanceStore.TryGetValue(creationMessage.OrchestrationInstance.InstanceId, out ed))
                {
                    ed = new Dictionary<string, OrchestrationState>();
                    this.instanceStore[creationMessage.OrchestrationInstance.InstanceId] = ed;
                }

                OrchestrationState newState = new OrchestrationState()
                {
                    OrchestrationInstance = new OrchestrationInstance
                    {
                        InstanceId = creationMessage.OrchestrationInstance.InstanceId,
                        ExecutionId = creationMessage.OrchestrationInstance.ExecutionId,
                    },
                    CreatedTime = DateTime.UtcNow,
                    OrchestrationStatus = OrchestrationStatus.Pending,
                    Version = ee.Version,
                    Name = ee.Name,
                    Input = ee.Input,
                };

                ed.Add(creationMessage.OrchestrationInstance.ExecutionId, newState);
            }

            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            return SendTaskOrchestrationMessageBatchAsync(message);
        }

        /// <inheritdoc />
        public Task SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages)
        {
            foreach (var message in messages)
            {
                this.orchestratorQueue.SendMessage(message);
            }

            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public async Task<OrchestrationState> WaitForOrchestrationAsync(
            string instanceId,
            string executionId,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(executionId))
            {
                executionId = string.Empty;
            }

            string key = instanceId + "_" + executionId;

            TaskCompletionSource<OrchestrationState> tcs = null;
            if (!this.orchestrationWaiters.TryGetValue(key, out tcs))
            {
                tcs = new TaskCompletionSource<OrchestrationState>();

                if (!this.orchestrationWaiters.TryAdd(key, tcs))
                {
                    this.orchestrationWaiters.TryGetValue(key, out tcs);
                }

                if (tcs == null)
                {
                    throw new InvalidOperationException("Unable to get tcs from orchestrationWaiters");
                }
            }

            // might have finished already
            lock (this.thisLock)
            {
                if (this.instanceStore.ContainsKey(instanceId))
                {
                    Dictionary<string, OrchestrationState> emap = this.instanceStore[instanceId];

                    if (emap != null && emap.Count > 0)
                    {
                        OrchestrationState state = null;
                        if (string.IsNullOrWhiteSpace(executionId))
                        {
                            IOrderedEnumerable<OrchestrationState> sortedemap = emap.Values.OrderByDescending(os => os.CreatedTime);
                            state = sortedemap.First();
                        }
                        else
                        {
                            if (emap.ContainsKey(executionId))
                            {
                                state = this.instanceStore[instanceId][executionId];
                            }
                        }

                        if (state != null
                            && state.OrchestrationStatus != OrchestrationStatus.Running
                            && state.OrchestrationStatus != OrchestrationStatus.Pending)
                        {
                            // if only master id was specified then continueasnew is a not a terminal state
                            if (!(string.IsNullOrWhiteSpace(executionId) && state.OrchestrationStatus == OrchestrationStatus.ContinuedAsNew))
                            {
                                tcs.TrySetResult(state);
                            }
                        }
                    }
                }
            }

            var cts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken,
                this.cancellationTokenSource.Token);
            Task timeOutTask = Task.Delay(timeout, cts.Token);
            Task ret = await Task.WhenAny(tcs.Task, timeOutTask);

            if (ret == timeOutTask)
            {
                throw new TimeoutException("timed out or canceled while waiting for orchestration to complete");
            }

            cts.Cancel();

            return await tcs.Task;
        }

        /// <inheritdoc />
        public async Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            OrchestrationState response;
            Dictionary<string, OrchestrationState> state;

            lock (this.thisLock)
            {
                if (!(this.instanceStore.TryGetValue(instanceId, out state) &&
                    state.TryGetValue(executionId, out response)))
                {
                    response = null;
                }
            }

            return await Task.FromResult(response);
        }

        /// <inheritdoc />
        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        {
            IList<OrchestrationState> response;
            Dictionary<string, OrchestrationState> state;

            lock (this.thisLock)
            {
                if (this.instanceStore.TryGetValue(instanceId, out state))
                {
                    response = state.Values.ToList();
                }
                else
                {
                    response = new List<OrchestrationState>();
                }
            }

            return await Task.FromResult(response);
        }

        /// <inheritdoc />
        public Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public Task PurgeOrchestrationHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotImplementedException();
        }

        /******************************/
        // Task orchestration methods
        /******************************/
        /// <inheritdoc />
        public int MaxConcurrentTaskOrchestrationWorkItems => MaxConcurrentWorkItems;

        /// <inheritdoc />
        public async Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            TaskSession taskSession = await this.orchestratorQueue.AcceptSessionAsync(receiveTimeout,
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, this.cancellationTokenSource.Token).Token);

            if (taskSession == null)
            {
                return null;
            }

            TaskOrchestrationWorkItem wi = new TaskOrchestrationWorkItem()
            {
                NewMessages = taskSession.Messages.ToList(),
                InstanceId = taskSession.Id,
                LockedUntilUtc = DateTime.UtcNow.AddMinutes(5),
                OrchestrationRuntimeState =
                    this.DeserializeOrchestrationRuntimeState(taskSession.SessionState) ??
                    new OrchestrationRuntimeState(),
            };

            return wi;
        }

        /// <inheritdoc />
        public Task CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState,
            IList<TaskMessage> outboundMessages,
            IList<TaskMessage> orchestratorMessages,
            IList<TaskMessage> workItemTimerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState state)
        {
            lock (this.thisLock)
            {
                this.orchestratorQueue.CompleteSession(
                    workItem.InstanceId,
                    newOrchestrationRuntimeState != null ?
                    this.SerializeOrchestrationRuntimeState(newOrchestrationRuntimeState) : null,
                    orchestratorMessages,
                    continuedAsNewMessage
                    );

                if (outboundMessages != null)
                {
                    foreach (TaskMessage m in outboundMessages)
                    {
                        // AFFANDAR : TODO : make async
                        this.workerQueue.SendMessageAsync(m);
                    }
                }

                if (workItemTimerMessages != null)
                {
                    lock (this.timerLock)
                    {
                        foreach (TaskMessage m in workItemTimerMessages)
                        {
                            this.timerMessages.Add(m);
                        }
                    }
                }

                if (state != null)
                {
                    Dictionary<string, OrchestrationState> ed;

                    if (!this.instanceStore.TryGetValue(workItem.InstanceId, out ed))
                    {
                        ed = new Dictionary<string, OrchestrationState>();
                        this.instanceStore[workItem.InstanceId] = ed;
                    }

                    ed[workItem.OrchestrationRuntimeState.OrchestrationInstance.ExecutionId] = state;

                    // signal any waiters waiting on instanceid_executionid or just the latest instanceid_
                    TaskCompletionSource<OrchestrationState> tcs = null;
                    TaskCompletionSource<OrchestrationState> tcs1 = null;

                    if (state.OrchestrationStatus == OrchestrationStatus.Running
                        || state.OrchestrationStatus == OrchestrationStatus.Pending)
                    {
                        return Task.FromResult(0);
                    }

                    string key = workItem.OrchestrationRuntimeState.OrchestrationInstance.InstanceId + "_" +
                        workItem.OrchestrationRuntimeState.OrchestrationInstance.ExecutionId;

                    string key1 = workItem.OrchestrationRuntimeState.OrchestrationInstance.InstanceId + "_";

                    var tasks = new List<Task>();


                    if (this.orchestrationWaiters.TryGetValue(key, out tcs))
                    {
                        tasks.Add(Task.Run(() => tcs.TrySetResult(state)));
                    }

                    // for instanceid level waiters, we will not consider continueasnew as a terminal state because
                    // the high level orch is still ongoing
                    if (state.OrchestrationStatus != OrchestrationStatus.ContinuedAsNew
                        && this.orchestrationWaiters.TryGetValue(key1, out tcs1))
                    {
                        tasks.Add(Task.Run(() => tcs1.TrySetResult(state)));
                    }

                    if (tasks.Count > 0)
                    {
                        Task.WaitAll(tasks.ToArray());
                    }
                }
            }

            return Task.FromResult(0);
        }

        /// <inheritdoc />
        public Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            this.orchestratorQueue.AbandonSession(workItem.InstanceId);
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public int TaskActivityDispatcherCount => 1;

        /// <inheritdoc />
        public int MaxConcurrentTaskActivityWorkItems => MaxConcurrentWorkItems;

        /// <inheritdoc />
        public async Task ForceTerminateTaskOrchestrationAsync(string instanceId, string message)
        {
            var taskMessage = new TaskMessage
            {
                OrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId },
                Event = new ExecutionTerminatedEvent(-1, message)
            };

            await SendTaskOrchestrationMessageAsync(taskMessage);
        }

        /// <inheritdoc />
        public Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            workItem.LockedUntilUtc = workItem.LockedUntilUtc.AddMinutes(5);
            return Task.FromResult(0);
        }

        /// <inheritdoc />
        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            return false;
        }

        /// <inheritdoc />
        public int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            return 0;
        }

        /// <inheritdoc />
        public int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            return 0;
        }

        /// <inheritdoc />
        public int TaskOrchestrationDispatcherCount => 1;

        /******************************/
        // Task activity methods
        /******************************/
        /// <inheritdoc />
        public async Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            TaskMessage taskMessage = await this.workerQueue.ReceiveMessageAsync(receiveTimeout,
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, this.cancellationTokenSource.Token).Token);

            if (taskMessage == null)
            {
                return null;
            }

            return new TaskActivityWorkItem
            {
                Id = "N/A",        // for the inmem provider we will just use the TaskMessage object ref itself as the id
                LockedUntilUtc = DateTime.UtcNow.AddMinutes(5),
                TaskMessage = taskMessage,
            };
        }

        /// <inheritdoc />
        public Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            this.workerQueue.AbandonMessageAsync(workItem.TaskMessage);
            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
        {
            lock (this.thisLock)
            {
                this.workerQueue.CompleteMessageAsync(workItem.TaskMessage);
                this.orchestratorQueue.SendMessage(responseMessage);
            }

            return Task.FromResult<object>(null);
        }

        /// <inheritdoc />
        public Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            // AFFANDAR : TODO : add expiration if we want to unit test it
            workItem.LockedUntilUtc = workItem.LockedUntilUtc.AddMinutes(5);
            return Task.FromResult(workItem);
        }

        byte[] SerializeOrchestrationRuntimeState(OrchestrationRuntimeState runtimeState)
        {
            if (runtimeState == null)
            {
                return null;
            }

            string serializeState = JsonConvert.SerializeObject(runtimeState.Events,
                new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.Auto });
            return Encoding.UTF8.GetBytes(serializeState);
        }

        OrchestrationRuntimeState DeserializeOrchestrationRuntimeState(byte[] stateBytes)
        {
            if (stateBytes == null || stateBytes.Length == 0)
            {
                return null;
            }

            string serializedState = Encoding.UTF8.GetString(stateBytes);
            IList<HistoryEvent> events = JsonConvert.DeserializeObject<IList<HistoryEvent>>(serializedState, new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.Auto });
            return new OrchestrationRuntimeState(events);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.cancellationTokenSource.Cancel();
                this.cancellationTokenSource.Dispose();
            }
        }
    }
}