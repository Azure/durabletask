using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask;
using DurableTask.History;
using Newtonsoft.Json;

namespace FrameworkUnitTests.PrototypeMocks
{
    class SynchronousInMemoryOrchestrationService : IOrchestrationService, IOrchestrationServiceClient
    {
        Queue<TaskMessage> _workerQueue = new Queue<TaskMessage>();
        Dictionary<string, SessionDocument> _orchestrationSessions = new Dictionary<string, SessionDocument>();

        Dictionary<string, Dictionary<string, OrchestrationState>> _instanceStore = new Dictionary<string, Dictionary<string, OrchestrationState>>();
        ConcurrentDictionary<string, TaskCompletionSource<OrchestrationState>> _orchestrationWaiters = new ConcurrentDictionary<string, TaskCompletionSource<OrchestrationState>>();
        CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();

        SessionDocument _peekedSession = null;
        TaskMessage _peekedActivity = null;

        object _thisLock = new object();

        public Task StartAsync()
        {
            return Task.FromResult<object>(null);
        }

        public Task StopAsync()
        {
            return StopAsync(false);
        }

        public Task StopAsync(bool isForced)
        {
            this._cancellationTokenSource.Cancel();
            return Task.FromResult<object>(null);
        }

        public Task CreateAsync()
        {
            return CreateAsync(true);
        }

        public Task CreateAsync(bool recreateInstanceStore)
        {
            return Task.FromResult<object>(null);
        }

        public Task CreateIfNotExistsAsync()
        {
            return Task.FromResult<object>(null);
        }

        public Task DeleteAsync()
        {
            return DeleteAsync(true);
        }

        public Task DeleteAsync(bool deleteInstanceStore)
        {
            return Task.FromResult<object>(null);
        }

        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            return false;
        }

        public int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            return 0;
        }

        public int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            return 0;
        }

        public int TaskOrchestrationDispatcherCount => 1;
        public int MaxConcurrentTaskOrchestrationWorkItems => 1;
        public Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            //lock (_thisLock)
            {
                Contract.Assert(_peekedSession == null, "How come we get a call for another session while a session is being processed?");

                // Todo: This is the bad part - this really is a look up for the first session with no lock and some messages to process and it requires O(n) of dictionary
                foreach (var entry in _orchestrationSessions)
                {
                    if (!entry.Value.AnyActiveMessages())
                    {
                        continue;
                    }

                    _peekedSession = entry.Value;

                    return Task.FromResult(new TaskOrchestrationWorkItem()
                    {
                        NewMessages = _peekedSession.LockMessages(),
                        InstanceId = _peekedSession.Id,
                        OrchestrationRuntimeState =
                            this.DeserializeOrchestrationRuntimeState(_peekedSession.SessionState) ??
                            new OrchestrationRuntimeState()
                    });
                }
            }

            return Task.FromResult<TaskOrchestrationWorkItem>(null);
        }

        public Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public Task CompleteTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem,
            OrchestrationRuntimeState newOrchestrationRuntimeState,
            IList<TaskMessage> outboundMessages,
            IList<TaskMessage> orchestratorMessages,
            IList<TaskMessage> timerMessages,
            TaskMessage continuedAsNewMessage,
            OrchestrationState orchestrationState)
        {
            Contract.Assert(string.Equals(_peekedSession.Id, workItem.InstanceId), "Unexpected thing happened, complete should be called with the same session as locked");

            //if (timerMessages != null)
            //{
            //    throw new ArgumentException("timer messages not supported");
            //}

            //if (orchestratorMessages != null)
            //{
            //    throw new ArgumentException("Sub orchestrations are not supported");
            //}

            //if (continuedAsNewMessage != null)
            //{
            //    throw new ArgumentException("Continued as new is not supported");
            //}

            lock (_thisLock)
            {
                _peekedSession.SessionState = this.SerializeOrchestrationRuntimeState(newOrchestrationRuntimeState);

                foreach (var m in outboundMessages)
                {
                    _workerQueue.Enqueue(m);
                }

                _peekedSession.CompleteLocked();
                var state = orchestrationState;
                _peekedSession = null;
                if (state != null)
                {
                    Dictionary<string, OrchestrationState> ed;

                    if (!this._instanceStore.TryGetValue(workItem.InstanceId, out ed))
                    {
                        ed = new Dictionary<string, OrchestrationState>();
                        this._instanceStore[workItem.InstanceId] = ed;
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


                    if (this._orchestrationWaiters.TryGetValue(key, out tcs))
                    {
                        tasks.Add(Task.Run(() => tcs.TrySetResult(state)));
                    }

                    // for instanceid level waiters, we will not consider continueasnew as a terminal state because
                    // the high level orch is still ongoing
                    if (state.OrchestrationStatus != OrchestrationStatus.ContinuedAsNew
                        && this._orchestrationWaiters.TryGetValue(key1, out tcs1))
                    {
                        tasks.Add(Task.Run(() => tcs1.TrySetResult(state)));
                    }

                    if (tasks.Count > 0)
                    {
                        Task.WaitAll(tasks.ToArray());
                    }
                }

            }

            return Task.FromResult<object>(null);
        }

        public Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            //lock (_thisLock)
            {
                _peekedSession = null;
            }
            return Task.FromResult<object>(null);
        }

        public Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            return Task.FromResult<object>(null);
        }

        public int TaskActivityDispatcherCount => 1;
        public int MaxConcurrentTaskActivityWorkItems => 1;

        public Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            Contract.Assert(_peekedActivity == null, "How come we get a call for another activity while an activity is running?");

            //lock (_thisLock)
            {
                if (!_workerQueue.Any()) return Task.FromResult<TaskActivityWorkItem>(null);

                _peekedActivity = _workerQueue.Peek();
            }

            return Task.FromResult(new TaskActivityWorkItem()
            {
                Id = "N/A",
                TaskMessage = _peekedActivity
            });
        }

        public Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            Contract.Assert(workItem.TaskMessage == _peekedActivity, "Unexpected thing happened, renew lock called for an activity that's not the current activity");
            return Task.FromResult(workItem);
        }

        public Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
        {
            Contract.Assert(workItem.TaskMessage == _peekedActivity, "Unexpected thing happened, complete called for an activity that's not the current activity");

            lock (_thisLock)
            {
                var sessionId = workItem.TaskMessage.OrchestrationInstance.InstanceId;
                _orchestrationSessions[sessionId].AppendMessage(responseMessage);
                _workerQueue.Dequeue();
                _peekedActivity = null;
            }

            return Task.FromResult<object>(null);
        }

        public Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            //lock (_thisLock)
            {
                _peekedActivity = null;
            }
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

        /******************************/
        // client methods
        /******************************/

        public Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            ExecutionStartedEvent ee = creationMessage.Event as ExecutionStartedEvent;

            if (ee == null)
            {
                throw new InvalidOperationException("Invalid creation task message");
            }

            lock (this._thisLock)
            {
                SessionDocument session;
                if (!_orchestrationSessions.TryGetValue(creationMessage.OrchestrationInstance.InstanceId, out session))
                {
                    _orchestrationSessions.Add(creationMessage.OrchestrationInstance.InstanceId, new SessionDocument()
                    {
                        Id = creationMessage.OrchestrationInstance.InstanceId,
                        SessionState = this.SerializeOrchestrationRuntimeState(new OrchestrationRuntimeState())
                    });
                }
                _orchestrationSessions[creationMessage.OrchestrationInstance.InstanceId].AppendMessage(creationMessage);

                Dictionary<string, OrchestrationState> ed;

                if (!this._instanceStore.TryGetValue(creationMessage.OrchestrationInstance.InstanceId, out ed))
                {
                    ed = new Dictionary<string, OrchestrationState>();
                    this._instanceStore[creationMessage.OrchestrationInstance.InstanceId] = ed;
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

        public Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            throw new NotImplementedException();
        }

        public async Task<OrchestrationState> WaitForOrchestrationAsync(string instanceId, string executionId, TimeSpan timeout, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(executionId))
            {
                executionId = string.Empty;
            }

            string key = instanceId + "_" + executionId;

            TaskCompletionSource<OrchestrationState> tcs = null;
            if (!this._orchestrationWaiters.TryGetValue(key, out tcs))
            {
                tcs = new TaskCompletionSource<OrchestrationState>();

                if (!this._orchestrationWaiters.TryAdd(key, tcs))
                {
                    this._orchestrationWaiters.TryGetValue(key, out tcs);
                }

                if (tcs == null)
                {
                    throw new InvalidOperationException("Unable to get tcs from orchestrationWaiters");
                }
            }

            // might have finished already
            lock (this._thisLock)
            {
                if (this._instanceStore.ContainsKey(instanceId))
                {
                    Dictionary<string, OrchestrationState> emap = this._instanceStore[instanceId];

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
                                state = this._instanceStore[instanceId][executionId];
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
                this._cancellationTokenSource.Token);
            Task timeOutTask = Task.Delay(timeout, cts.Token);
            Task ret = await Task.WhenAny(tcs.Task, timeOutTask);

            if (ret == timeOutTask)
            {
                throw new TimeoutException("timed out or canceled while waiting for orchestration to complete");
            }

            cts.Cancel();

            return await tcs.Task;
        }

        public Task ForceTerminateTaskOrchestrationAsync(string instanceId, string reason)
        {
            throw new NotImplementedException();
        }

        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        {
            IList<OrchestrationState> response = null;
            lock (this._thisLock)
            {
                if (this._instanceStore[instanceId] != null)
                {
                    response = this._instanceStore[instanceId].Values.ToList();
                }
                else
                {
                    response = new List<OrchestrationState>();
                }
            }

            return await Task.FromResult(response);
        }

        public async Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            OrchestrationState response = null;
            lock (this._thisLock)
            {
                if (this._instanceStore[instanceId] != null)
                {
                    response = this._instanceStore[instanceId][executionId];
                }
            }

            return await Task.FromResult(response);
        }

        public Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }

        public Task PurgeOrchestrationHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotImplementedException();
        }
    }
}
