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

namespace DurableTask.ServiceFabric
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Data;
    using Microsoft.ServiceFabric.Data.Collections;

    class SessionsProvider : MessageProviderBase<string, PersistentSession>
    {
        ConcurrentQueue<string> inMemorySessionsQueue = new ConcurrentQueue<string>();
        ConcurrentDictionary<string, LockState> lockedSessions = new ConcurrentDictionary<string, LockState>();

        ConcurrentDictionary<OrchestrationInstance, SessionMessagesProvider<Guid, TaskMessage>> sessionMessageProviders = new ConcurrentDictionary<OrchestrationInstance, SessionMessagesProvider<Guid, TaskMessage>>(OrchestrationInstanceComparer.Default);

        public SessionsProvider(IReliableStateManager stateManager, CancellationToken token) : base(stateManager, Constants.OrchestrationDictionaryName, token)
        {
        }

        public async Task<PersistentSession> AcceptSessionAsync(TimeSpan receiveTimeout)
        {
            if (!IsStopped())
            {
                string returnInstanceId;
                bool newItemsBeforeTimeout = true;
                while (newItemsBeforeTimeout)
                {
                    if (this.inMemorySessionsQueue.TryDequeue(out returnInstanceId))
                    {
                        try
                        {
                            using (var txn = this.StateManager.CreateTransaction())
                            {
                                var existingValue = await this.Store.TryGetValueAsync(txn, returnInstanceId);

                                if (existingValue.HasValue)
                                {
                                    if (!this.lockedSessions.TryUpdate(returnInstanceId, newValue: LockState.Locked, comparisonValue: LockState.InFetchQueue))
                                    {
                                        var errorMessage = $"Internal Server Error : Unexpected to dequeue the session {returnInstanceId} which was already locked before";
                                        ProviderEventSource.Log.UnexpectedCodeCondition(errorMessage);
                                        throw new Exception(errorMessage);
                                    }

                                    ProviderEventSource.Log.TraceMessage(returnInstanceId, "Session Locked Accepted");
                                    return existingValue.Value;
                                }
                                else
                                {
                                    var errorMessage = $"Internal Server Error: Did not find the session object in reliable dictionary while having the session {returnInstanceId} in memory";
                                    ProviderEventSource.Log.UnexpectedCodeCondition(errorMessage);
                                    throw new Exception(errorMessage);
                                }
                            }
                        }
                        catch (Exception)
                        {
                            this.inMemorySessionsQueue.Enqueue(returnInstanceId);
                            throw;
                        }
                    }

                    newItemsBeforeTimeout = await WaitForItemsAsync(receiveTimeout);
                }
            }
            return null;
        }

        protected override void AddItemInMemory(string key, PersistentSession value)
        {
            this.TryEnqueueSession(key);
        }

        public async Task<List<Message<Guid, TaskMessage>>> ReceiveSessionMessagesAsync(PersistentSession session)
        {
            var sessionMessageProvider = await GetOrAddSessionMessagesInstance(session.SessionId);
            var messages = await sessionMessageProvider.ReceiveBatchAsync();
            ProviderEventSource.Log.TraceMessage(session.SessionId.InstanceId, $"Number of received messages {messages.Count}");
            return messages;
        }

        public async Task CompleteAndUpdateSession(ITransaction transaction,
            OrchestrationInstance instance,
            OrchestrationRuntimeState newSessionState,
            List<Guid> lockTokens)
        {
            SessionMessagesProvider<Guid, TaskMessage> sessionMessageProvider;
            if (this.sessionMessageProviders.TryGetValue(instance, out sessionMessageProvider))
            {
                ProviderEventSource.Log.TraceMessage(instance.InstanceId, $"Number of completed messages {lockTokens.Count}");
                await sessionMessageProvider.CompleteBatchAsync(transaction, lockTokens);
            }
            else
            {
                ProviderEventSource.Log.UnexpectedCodeCondition($"{nameof(SessionsProvider)}.{nameof(CompleteAndUpdateSession)} : Did not find session messages provider instance for session : {instance}.");
            }

            var sessionStateEvents = newSessionState?.Events.ToImmutableList();
            var result = PersistentSession.Create(instance, sessionStateEvents);
#if DEBUG
            ProviderEventSource.Log.LogSizeMeasure($"Value in SessionsProvider for SessionId = {instance.InstanceId}", DebugSerializationUtil.GetDataContractSerializationSize(result));
#endif
            await this.Store.SetAsync(transaction, instance.InstanceId, result);
        }

        public async Task AppendMessageAsync(TaskMessage newMessage)
        {
            await RetryHelper.ExecuteWithRetryOnTransient(async () =>
            {
                using (var txn = this.StateManager.CreateTransaction())
                {
                    await this.AppendMessageAsync(txn, newMessage);
                    await txn.CommitAsync();
                }
            }, uniqueActionIdentifier: $"OrchestrationId = '{newMessage.OrchestrationInstance.InstanceId}', Action = '{nameof(SessionsProvider)}.{nameof(AppendMessageAsync)}'");

            this.TryEnqueueSession(newMessage.OrchestrationInstance);
        }

        public async Task AppendMessageAsync(ITransaction transaction, TaskMessage newMessage)
        {
            ThrowIfStopped();
            await EnsureOrchestrationStoreInitialized();

            var sessionMessageProvider = await GetOrAddSessionMessagesInstance(newMessage.OrchestrationInstance);
            await sessionMessageProvider.SendBeginAsync(transaction, new Message<Guid, TaskMessage>(Guid.NewGuid(), newMessage));
            await this.Store.TryAddAsync(transaction, newMessage.OrchestrationInstance.InstanceId, PersistentSession.Create(newMessage.OrchestrationInstance));
        }

        public async Task<bool> TryAppendMessageAsync(ITransaction transaction, TaskMessage newMessage)
        {
            ThrowIfStopped();

            if (await this.Store.ContainsKeyAsync(transaction, newMessage.OrchestrationInstance.InstanceId))
            {
                var sessionMessageProvider = await GetOrAddSessionMessagesInstance(newMessage.OrchestrationInstance);
                await sessionMessageProvider.SendBeginAsync(transaction, new Message<Guid, TaskMessage>(Guid.NewGuid(), newMessage));
                return true;
            }

            return false;
        }

        public async Task<IList<OrchestrationInstance>> TryAppendMessageBatchAsync(ITransaction transaction, IEnumerable<TaskMessage> newMessages)
        {
            ThrowIfStopped();
            List<OrchestrationInstance> modifiedSessions = new List<OrchestrationInstance>();

            var groups = newMessages.GroupBy(m => m.OrchestrationInstance, OrchestrationInstanceComparer.Default);

            foreach (var group in groups)
            {
                if (await this.Store.ContainsKeyAsync(transaction, group.Key.InstanceId))
                {
                    var sessionMessageProvider = await GetOrAddSessionMessagesInstance(group.Key);
                    await sessionMessageProvider.SendBatchBeginAsync(transaction, group.Select(tm => new Message<Guid, TaskMessage>(Guid.NewGuid(), tm)));
                    modifiedSessions.Add(group.Key);
                }
            }

            return modifiedSessions;
        }

        public async Task AppendMessageBatchAsync(ITransaction transaction, IEnumerable<TaskMessage> newMessages)
        {
            ThrowIfStopped();
            var groups = newMessages.GroupBy(m => m.OrchestrationInstance, OrchestrationInstanceComparer.Default);

            foreach (var group in groups)
            {
                var groupMessages = group.AsEnumerable();

                await this.Store.TryAddAsync(transaction, group.Key.InstanceId, PersistentSession.Create(group.Key));
                var sessionMessageProvider = await GetOrAddSessionMessagesInstance(group.Key);
                await sessionMessageProvider.SendBatchBeginAsync(transaction, group.Select(tm => new Message<Guid, TaskMessage>(Guid.NewGuid(), tm)));
            }
        }

        public async Task<IEnumerable<PersistentSession>> GetSessions()
        {
            await EnsureOrchestrationStoreInitialized();

            var result = new List<PersistentSession>();
            await this.EnumerateItems(kvp => result.Add(kvp.Value));
            return result;
        }

        public void TryUnlockSession(OrchestrationInstance instance, bool abandon = false, bool isComplete = false)
        {
            ProviderEventSource.Log.TraceMessage(instance.InstanceId, $"Session Unlock Begin, Abandon = {abandon}");
            LockState lockState;
            if (!this.lockedSessions.TryRemove(instance.InstanceId, out lockState) || lockState == LockState.InFetchQueue)
            {
                var errorMessage = $"{nameof(SessionsProvider)}.{nameof(TryUnlockSession)} : Trying to unlock the session {instance.InstanceId} which was not locked.";
                ProviderEventSource.Log.UnexpectedCodeCondition(errorMessage);
                throw new Exception(errorMessage);
            }

            if (!isComplete && (abandon || lockState == LockState.NewMessagesWhileLocked))
            {
                this.TryEnqueueSession(instance);
            }
            ProviderEventSource.Log.TraceMessage(instance.InstanceId, $"Session Unlock End, Abandon = {abandon}, removed lock state = {lockState}");
        }

        public async Task<bool> SessionExists(OrchestrationInstance instance)
        {
            await EnsureOrchestrationStoreInitialized();

            using (var txn = this.StateManager.CreateTransaction())
            {
                return await this.Store.ContainsKeyAsync(txn, instance.InstanceId);
            }
        }

        public void TryEnqueueSession(OrchestrationInstance instance)
        {
            this.TryEnqueueSession(instance.InstanceId);
        }

        void TryEnqueueSession(string instanceId)
        {
            bool enqueue = true;
            this.lockedSessions.AddOrUpdate(instanceId, LockState.InFetchQueue, (ses, old) =>
            {
                enqueue = false;
                return old == LockState.Locked ? LockState.NewMessagesWhileLocked : old;
            });

            if (enqueue)
            {
                ProviderEventSource.Log.TraceMessage(instanceId, "Session Getting Enqueued");
                this.inMemorySessionsQueue.Enqueue(instanceId);
                SetWaiterForNewItems();
            }
        }

        public async Task DropSession(ITransaction txn, OrchestrationInstance instance)
        {
            if (instance == null)
            {
                throw new ArgumentNullException(nameof(instance));
            }

            SessionMessagesProvider<Guid, TaskMessage> sessionMessagesProvider;
            this.sessionMessageProviders.TryRemove(instance, out sessionMessagesProvider);

            var noWait = Task.Run(() => this.StateManager.RemoveAsync(GetSessionMessagesDictionaryName(instance)));
            await this.Store.TryRemoveAsync(txn, instance.InstanceId);
        }

        Task EnsureOrchestrationStoreInitialized()
        {
            //Workaround to avoid client sending a new message before StartAsync on service is done
            if (this.Store == null)
            {
                return InitializeStore();
            }

            return CompletedTask.Default;
        }

        async Task<SessionMessagesProvider<Guid, TaskMessage>> GetOrAddSessionMessagesInstance(OrchestrationInstance instance)
        {
            var newInstance = new SessionMessagesProvider<Guid, TaskMessage>(this.StateManager, GetSessionMessagesDictionaryName(instance), this.CancellationToken);
            var sessionMessageProvider = this.sessionMessageProviders.GetOrAdd(instance, newInstance);

            if (sessionMessageProvider == newInstance)
            {
                await sessionMessageProvider.StartAsync();
            }

            return sessionMessageProvider;
        }

        string GetSessionMessagesDictionaryName(OrchestrationInstance instance)
        {
            var sessionKey = $"{instance.InstanceId}_{instance.ExecutionId}";
            return Constants.SessionMessagesDictionaryPrefix + sessionKey;
        }

        enum LockState
        {
            InFetchQueue = 0,

            Locked,

            NewMessagesWhileLocked
        }
    }
}
