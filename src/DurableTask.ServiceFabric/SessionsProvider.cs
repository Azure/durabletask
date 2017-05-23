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
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Data;

    class SessionsProvider : MessageProviderBase<string, PersistentSession>
    {
        ConcurrentQueue<string> inMemorySessionsQueue = new ConcurrentQueue<string>();
        ConcurrentDictionary<string, LockState> lockedSessions = new ConcurrentDictionary<string, LockState>();

        ConcurrentDictionary<string, SessionMessagesProvider<Guid, TaskMessage>> sessionMessageProviders = new ConcurrentDictionary<string, SessionMessagesProvider<Guid, TaskMessage>>();
        ConcurrentDictionary<string, List<Guid>> lockedSessionMessageTokens = new ConcurrentDictionary<string, List<Guid>>();

        public SessionsProvider(IReliableStateManager stateManager) : base(stateManager, Constants.OrchestrationDictionaryName)
        {
        }

        public async Task<PersistentSession> AcceptSessionAsync(TimeSpan receiveTimeout)
        {
            if (!IsStopped())
            {
                string returnSessionId;
                bool newItemsBeforeTimeout = true;
                while (newItemsBeforeTimeout)
                {
                    if (this.inMemorySessionsQueue.TryDequeue(out returnSessionId))
                    {
                        try
                        {
                            using (var txn = this.StateManager.CreateTransaction())
                            {
                                var existingValue = await this.Store.TryGetValueAsync(txn, returnSessionId);

                                if (existingValue.HasValue)
                                {
                                    if (!this.lockedSessions.TryUpdate(returnSessionId, newValue: LockState.Locked, comparisonValue: LockState.InFetchQueue))
                                    {
                                        var errorMessage = $"Internal Server Error : Unexpected to dequeue the session {returnSessionId} which was already locked before";
                                        ProviderEventSource.Log.UnexpectedCodeCondition(errorMessage);
                                        throw new Exception(errorMessage);
                                    }

                                    ProviderEventSource.Log.TraceMessage(returnSessionId, "Session Locked Accepted");
                                    return existingValue.Value;
                                }
                                else
                                {
                                    var errorMessage = $"Internal Server Error: Did not find the session object in reliable dictionary while having the session {returnSessionId} in memory";
                                    ProviderEventSource.Log.UnexpectedCodeCondition(errorMessage);
                                    throw new Exception(errorMessage);
                                }
                            }
                        }
                        catch (Exception)
                        {
                            this.inMemorySessionsQueue.Enqueue(returnSessionId);
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

        async Task<SessionMessagesProvider<Guid, TaskMessage>> GetOrAddSessionMessagesInstance(string sessionId)
        {
            var newInstance = new SessionMessagesProvider<Guid, TaskMessage>(this.StateManager, sessionId);
            var sessionMessageProvider = this.sessionMessageProviders.GetOrAdd(sessionId, newInstance);

            if (sessionMessageProvider == newInstance)
            {
                await sessionMessageProvider.StartAsync();
            }

            return sessionMessageProvider;
        }

        public async Task<List<TaskMessage>> ReceiveSessionMessagesAsync(PersistentSession session)
        {
            var sessionMessageProvider = await GetOrAddSessionMessagesInstance(session.SessionId);
            var messages = await sessionMessageProvider.ReceiveBatchAsync();
            ProviderEventSource.Log.TraceMessage(session.SessionId, $"Number of received messages {messages.Count}");
            if (!this.lockedSessionMessageTokens.TryAdd(session.SessionId, messages.Select(m => m.Key).ToList()))
            {
                ProviderEventSource.Log.UnexpectedCodeCondition($"{nameof(SessionsProvider)}.{nameof(ReceiveSessionMessagesAsync)} : Multiple receivers processing the same session : {session.SessionId}?");
                return new List<TaskMessage>();
            }
            return messages.Select(m => m.Value).ToList();
        }

        /// <summary>
        /// Callers should pass the transaction and once the transaction is commited successfully,
        /// should call <see cref="DurableTask.ServiceFabric.SessionsProvider.TryUnlockSession"/>.
        /// </summary>
        public async Task CompleteAndUpdateSession(ITransaction transaction,
            string sessionId,
            OrchestrationRuntimeState newSessionState)
        {
            List<Guid> lockTokens;
            if (this.lockedSessionMessageTokens.TryGetValue(sessionId, out lockTokens))
            {
                SessionMessagesProvider<Guid, TaskMessage> sessionMessageProvider;
                if (this.sessionMessageProviders.TryGetValue(sessionId, out sessionMessageProvider))
                {
                    ProviderEventSource.Log.TraceMessage(sessionId, $"Number of completed messages {lockTokens.Count}");
                    await sessionMessageProvider.CompleteBatchAsync(transaction, lockTokens);
                }
                else
                {
                    ProviderEventSource.Log.UnexpectedCodeCondition($"{nameof(SessionsProvider)}.{nameof(CompleteAndUpdateSession)} : Did not find session messages provider instance for session : {sessionId}.");
                }
            }
            else
            {
                ProviderEventSource.Log.UnexpectedCodeCondition($"{nameof(SessionsProvider)}.{nameof(CompleteAndUpdateSession)} : Did not find lock tokens dictionary for session messages for session : {sessionId}.");
            }

            var sessionStateEvents = newSessionState?.Events.ToImmutableList();
            var result = PersistentSession.Create(sessionId, sessionStateEvents);
#if DEBUG
            ProviderEventSource.Log.LogSizeMeasure($"Value in SessionsProvider for SessionId = {sessionId}", DebugSerializationUtil.GetDataContractSerializationSize(result));
#endif
            await this.Store.SetAsync(transaction, sessionId, result);
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

            this.TryEnqueueSession(newMessage.OrchestrationInstance.InstanceId);
        }

        public async Task AppendMessageAsync(ITransaction transaction, TaskMessage newMessage)
        {
            ThrowIfStopped();
            await EnsureOrchestrationStoreInitialized();

            var sessionId = newMessage.OrchestrationInstance.InstanceId;
            var sessionMessageProvider = await GetOrAddSessionMessagesInstance(sessionId);
            await sessionMessageProvider.SendBeginAsync(transaction, new Message<Guid, TaskMessage>(Guid.NewGuid(), newMessage));
            await this.Store.TryAddAsync(transaction, sessionId, PersistentSession.Create(sessionId));
        }

        public async Task<bool> TryAppendMessageAsync(ITransaction transaction, TaskMessage newMessage)
        {
            ThrowIfStopped();

            var sessionId = newMessage.OrchestrationInstance.InstanceId;
            if (await this.Store.ContainsKeyAsync(transaction, sessionId))
            {
                var sessionMessageProvider = await GetOrAddSessionMessagesInstance(newMessage.OrchestrationInstance.InstanceId);
                await sessionMessageProvider.SendBeginAsync(transaction, new Message<Guid, TaskMessage>(Guid.NewGuid(), newMessage));
                return true;
            }

            return false;
        }

        public async Task<IList<string>> TryAppendMessageBatchAsync(ITransaction transaction, IEnumerable<TaskMessage> newMessages)
        {
            ThrowIfStopped();
            List<string> modifiedSessions = new List<string>();

            var groups = newMessages.GroupBy(m => m.OrchestrationInstance.InstanceId);

            foreach (var group in groups)
            {
                if (await this.Store.ContainsKeyAsync(transaction, group.Key))
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
            var groups = newMessages.GroupBy(m => m.OrchestrationInstance.InstanceId);

            foreach (var group in groups)
            {
                var groupMessages = group.AsEnumerable();

                await this.Store.TryAddAsync(transaction, group.Key, PersistentSession.Create(group.Key));
                var sessionMessageProvider = await GetOrAddSessionMessagesInstance(group.Key);
                await sessionMessageProvider.SendBatchBeginAsync(transaction, group.Select(tm => new Message<Guid, TaskMessage>(Guid.NewGuid(), tm)));
            }
        }

        public void TryUnlockSession(string sessionId, bool abandon = false)
        {
            ProviderEventSource.Log.TraceMessage(sessionId, $"Session Unlock Begin, Abandon = {abandon}");
            LockState lockState;
            if (!this.lockedSessions.TryRemove(sessionId, out lockState) || lockState == LockState.InFetchQueue)
            {
                var errorMessage = $"{nameof(SessionsProvider)}.{nameof(TryUnlockSession)} : Trying to unlock the session {sessionId} which was not locked.";
                ProviderEventSource.Log.UnexpectedCodeCondition(errorMessage);
                throw new Exception(errorMessage);
            }

            List<Guid> ignored;
            this.lockedSessionMessageTokens.TryRemove(sessionId, out ignored);

            if (abandon || lockState == LockState.NewMessagesWhileLocked)
            {
                this.TryEnqueueSession(sessionId);
            }
            ProviderEventSource.Log.TraceMessage(sessionId, $"Session Unlock End, Abandon = {abandon}, removed lock state = {lockState}");
        }

        public async Task<bool> SessionExists(string sessionId)
        {
            await EnsureOrchestrationStoreInitialized();

            using (var txn = this.StateManager.CreateTransaction())
            {
                return await this.Store.ContainsKeyAsync(txn, sessionId);
            }
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

        public void TryEnqueueSession(string sessionId)
        {
            bool enqueue = true;
            this.lockedSessions.AddOrUpdate(sessionId, LockState.InFetchQueue, (ses, old) =>
            {
                enqueue = false;
                return old == LockState.Locked ? LockState.NewMessagesWhileLocked : old;
            });

            if (enqueue)
            {
                ProviderEventSource.Log.TraceMessage(sessionId, "Session Getting Enqueued");
                this.inMemorySessionsQueue.Enqueue(sessionId);
                SetWaiterForNewItems();
            }
        }

        public Task DropSession(string sessionId)
        {
            return RetryHelper.ExecuteWithRetryOnTransient(async () =>
            {
                SessionMessagesProvider<Guid, TaskMessage> sessionMessagesProvider;
                if (this.sessionMessageProviders.TryRemove(sessionId, out sessionMessagesProvider))
                {
                    await sessionMessagesProvider.StopAsync();
                    await sessionMessagesProvider.DropStoreAsync();
                }

                using (var txn = this.StateManager.CreateTransaction())
                {
                    await this.Store.TryRemoveAsync(txn, sessionId);
                    await txn.CommitAsync();
                }
            }, uniqueActionIdentifier: $"OrchestrationId = '{sessionId}', Action = '{nameof(DropSession)}'");
        }

        enum LockState
        {
            InFetchQueue = 0,

            Locked,

            NewMessagesWhileLocked
        }
    }
}
