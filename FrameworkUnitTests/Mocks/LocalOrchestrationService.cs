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

namespace FrameworkUnitTests.Mocks
{
    using DurableTask;
    using DurableTask.History;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Fully functional in-proc orchestration service for testing
    /// </summary>
    public class LocalOrchestrationService : IOrchestrationService, IOrchestrationServiceClient
    {
        Dictionary<string, byte[]> sessionState;
        List<TaskMessage> timerMessages;

        // dictionary<instanceid, dictionary<executionid, orchestrationstate>>
        //Dictionary<string, Dictionary<string, OrchestrationState>> instanceStore;

        PeekLockSessionQueue orchestratorQueue;
        PeeklockQueue workerQueue;

        CancellationTokenSource cancellationTokenSource;

        Dictionary<string, Dictionary<string, OrchestrationState>> instanceStore;

        //Dictionary<string, Tuple<List<TaskMessage>, byte[]>> sessionLock;

        object thisLock = new object();

        public LocalOrchestrationService()
        {
            this.orchestratorQueue = new PeekLockSessionQueue();
            this.workerQueue = new PeeklockQueue();

            this.sessionState = new Dictionary<string, byte[]>();

            this.timerMessages = new List<TaskMessage>();
            this.instanceStore = new Dictionary<string, Dictionary<string, OrchestrationState>>();
            this.cancellationTokenSource = new CancellationTokenSource();
        }

        async Task TimerMessageSchedulerAsync()
        {
            while (!this.cancellationTokenSource.Token.IsCancellationRequested)
            {
                lock(this.thisLock)
                {
                    foreach(TaskMessage tm in this.timerMessages)
                    {
                        TimerFiredEvent te = tm.Event as TimerFiredEvent;

                        if(te == null)
                        {
                            // AFFANDAR : TODO : unobserved task exception
                            throw new InvalidOperationException("Invalid timer message");
                        }

                        if(te.FireAt <= DateTime.UtcNow)
                        {
                            this.orchestratorQueue.SendMessage(tm);
                            this.timerMessages.Remove(tm);
                        }
                    }
                }

                await Task.Delay(TimeSpan.FromSeconds(5));
            }

            return;
        }

        /******************************/
        // management methods
        /******************************/
        public Task CreateAsync()
        {
            return Task.FromResult<object>(null);
        }

        public Task CreateIfNotExistsAsync()
        {
            return Task.FromResult<object>(null);
        }

        public Task DeleteAsync()
        {
            return Task.FromResult<object>(null);
        }

        public Task StartAsync()
        {
            Task.Run(() => TimerMessageSchedulerAsync());
            return Task.FromResult<object>(null);
        }

        public Task StopAsync()
        {
            this.cancellationTokenSource.Cancel();
            return Task.FromResult<object>(null);
        }

        /******************************/
        // client methods
        /******************************/
        public Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            ExecutionStartedEvent ee = creationMessage.Event as ExecutionStartedEvent;

            if(ee == null)
            {
                throw new InvalidOperationException("Invalid creation task message");
            }

            lock(this.thisLock)
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
                    OrchestrationStatus = OrchestrationStatus.Running,      // AFFANDAR : TODO : we should have a 'pending' state.
                    Version = ee.Version,
                    Name = ee.Name,
                    Input = ee.Input,
                };

                ed.Add(creationMessage.OrchestrationInstance.ExecutionId, newState);
            }
            return Task.FromResult<object>(null);
        }

        public Task SendTaskOrchestrationMessage(TaskMessage message)
        {
            this.orchestratorQueue.SendMessage(message);
            return Task.FromResult<object>(null);
        }

        public async Task<OrchestrationState> WaitForOrchestrationAsync(string instanceId, string executionId, TimeSpan timeout, CancellationToken cancellationToken)
        {
            Stopwatch timer = Stopwatch.StartNew();
            while (timer.Elapsed < timeout && !cancellationToken.IsCancellationRequested)
            {
                lock (this.thisLock)
                {
                    if (this.instanceStore[instanceId] == null)
                    {
                        return null;
                    }

                    OrchestrationState state = this.instanceStore[instanceId][executionId];

                    if (state.OrchestrationStatus != OrchestrationStatus.Running)
                    {
                        return state;
                    }
                }

                await Task.Delay(TimeSpan.FromSeconds(1));
            }

            if (cancellationToken.IsCancellationRequested)
            {
                throw new TaskCanceledException();
            }

            throw new TimeoutException("Timed out waiting for orchestration to complete");
        }

        public async Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            lock(this.thisLock)
            {
                if(this.instanceStore[instanceId] == null)
                {
                    return null;
                }

                return this.instanceStore[instanceId][executionId];
            }
        }

        public async Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        {
            lock (this.thisLock)
            {
                if (this.instanceStore[instanceId] == null)
                {
                    return new List<OrchestrationState>();
                }

                return (IList<OrchestrationState>)this.instanceStore[instanceId].ToList();
            }
        }

        public Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }

        public Task PurgeOrchestrationInstanceHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotImplementedException();
        }

        /******************************/
        // Task orchestration methods
        /******************************/
        public async Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(
            TimeSpan receiveTimeout, 
            CancellationToken cancellationToken)
        {
            TaskSession taskSession = await this.orchestratorQueue.AcceptSessionAsync(receiveTimeout, cancellationToken);

            if(taskSession == null)
            {
                return null;
            }

            TaskOrchestrationWorkItem wi = new TaskOrchestrationWorkItem()
            {
                NewMessages = taskSession.Messages,
                InstanceId = taskSession.Id,
                LockedUntilUtc = DateTime.UtcNow.AddMinutes(5),
                OrchestrationRuntimeState = 
                    this.DeserializeOrchestrationRuntimeState(taskSession.SessionState) ?? 
                    new OrchestrationRuntimeState(),
            };

            return wi;
        }

        public async Task CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem, 
            OrchestrationRuntimeState newOrchestrationRuntimeState, 
            IList<TaskMessage> outboundMessages, 
            IList<TaskMessage> orchestratorMessages, 
            IList<TaskMessage> timerMessages,
            OrchestrationState state)
        {
            lock(this.thisLock)
            {
                this.orchestratorQueue.CompleteSession(
                    workItem.InstanceId,
                    newOrchestrationRuntimeState != null ? 
                    this.SerializeOrchestrationRuntimeState(newOrchestrationRuntimeState) : null,
                    orchestratorMessages
                    );

                foreach(TaskMessage m in outboundMessages)
                {
                    // AFFANDAR : TODO : make async
                    this.workerQueue.SendMessageAsync(m);
                }

                foreach(TaskMessage m in timerMessages)
                {
                    this.timerMessages.Add(m);
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
                }
            }

            return;
        }

        public Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            this.orchestratorQueue.AbandonSession(workItem.InstanceId);
            return Task.FromResult<object>(null);
        }
        public Task TerminateTaskOrchestrationAsync(TaskOrchestrationWorkItem workItem, bool force)
        {
            throw new NotImplementedException();
        }

        public Task<TaskOrchestrationWorkItem> RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            workItem.LockedUntilUtc = workItem.LockedUntilUtc.AddMinutes(5);
            return Task.FromResult(workItem);
        }

        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            return false;
        }


        /******************************/
        // Task activity methods
        /******************************/
        public async Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            TaskMessage taskMessage = await this.workerQueue.ReceiveMessageAsync(receiveTimeout, cancellationToken);

            if(taskMessage == null)
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

        public Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            this.workerQueue.AbandonMessageAsync(workItem.TaskMessage);
            return Task.FromResult<object>(null);
        }

        public Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
        {
            lock(this.thisLock)
            {
                this.workerQueue.CompleteMessageAsync(workItem.TaskMessage);
                this.orchestratorQueue.SendMessage(responseMessage);
            }

            return Task.FromResult<object>(null);
        }

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
    }
}