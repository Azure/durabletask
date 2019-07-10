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

namespace DurableTask.AzureServiceFabric.Stores
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using DurableTask.Core;
    using DurableTask.AzureServiceFabric.TaskHelpers;
    using DurableTask.AzureServiceFabric.Tracing;

    using Microsoft.ServiceFabric.Data;

    /// <summary>
    /// There are three interesting methods in this class that require some explanation.
    /// 'AcceptSession' : Intent is to fetch the next available session and hand it out to consumer.
    /// 'TryUnlockSession' : Intent is to indicate that the above consumer is done processing the session.
    /// 'TryEnqueueSession' : Intent is to make the session available for a consumer if needed.
    ///
    /// 'AcceptSession'  should give a session to only one consumer at a time (strict requirement).
    /// We should also give out a session with messages in it but this is somewhat loose requirement because consumer
    /// can handle the scenario where there are no messages. But we don't want to keep making the session available
    /// to pick up when there are no incoming messages. (So giving a session with no messages once or even twice is ok
    /// but that should not happening recurringly)
    ///
    /// Incoming messages can come at anytime, before the session picked up,
    /// after 'AcceptSession' gave out a session but the consumer read messages in it, after consumer read messages in it, or
    /// after the session finished processing by consumer.
    ///
    /// 'AcceptSession; will fetch from 'fetchQueue' and returns it (so 'fetchQueueu' should never have a given session twice in
    /// the queue otherwise 'one consumer at a time' is violated. Also, we should not keep adding to 'fetchQueue' unless the session
    /// is interesting i.e., it needs processing).
    ///
    /// lockedSessions with 'LockState' is used to achieve the above.
    ///
    /// 'AcceptSession' fetches from 'fetchQueue' and transitions the session from 'InFetchQueue' to 'Locked'.
    /// (The implementation contract is such that if an item is in 'fetchQueue', it should be in 'InFetchQueue' state in lockedSessions)
    ///
    /// 'TryEnqueuSession' will try to add to lockedSessions with 'InFetchQueue' and only first such call after the session is unlocked will be
    /// able to add and that call will add the session back to 'fetchQueue'.
    /// If there is an entry in lockedSessions, 'TryEnqueuSession' does only one thing -
    /// if the session is already 'Locked' (that means a consumer has the session),
    /// it will just transition the state to 'NewMessagesWhileLocked' (so that session could be enqueued back again as it gets unlocked).
    /// Note that once the session is in this state, subsequent calls to 'TryEnqueueSession' before unlock will keep it in this state.
    /// Also Note that if session is in 'InFetchQueue' state, TryEnqueuSession does not need to (and should not) change the state.
    ///
    /// 'TryUnlockSession' first removes the session from lockedSessions [unlock complete at this point]
    /// but if the removed state is 'NewMessagesWhileLocked', it attempts to put it back on fetchQueue by calling 'TryEnqueueSession'.
    /// Note that at this point if another new incoming message calls 'TryEnqueueSession' only one of these should be able to add to 'fetchQueue'.
    ///
    /// Note that if new messages were sent after 'AcceptSession' but before consumer reads messages from session, we
    /// will hand out those messages but still add the session to fetchQueue (as part of unlock) and so the next fetch sees the session but
    /// with no messages. Though this can be made perfect by introducing one more state to 'LockedState', since consumer
    /// can handle no messages scenario, it's simpler to ignore the difference between the above and new messages being sent
    /// after consumer reads messages but before the session is unlocked.
    /// </summary>
    class SessionProvider : MessageProviderBase<string, PersistentSession>
    {
        ConcurrentQueue<string> fetchQueue = new ConcurrentQueue<string>();
        ConcurrentDictionary<string, LockState> lockedSessions = new ConcurrentDictionary<string, LockState>();

        ConcurrentDictionary<OrchestrationInstance, SessionMessageProvider> sessionMessageProviders
            = new ConcurrentDictionary<OrchestrationInstance, SessionMessageProvider>(OrchestrationInstanceComparer.Default);

        public SessionProvider(IReliableStateManager stateManager, CancellationToken token) : base(stateManager, Constants.OrchestrationDictionaryName, token)
        {
        }

        public async Task<PersistentSession> AcceptSessionAsync(TimeSpan receiveTimeout)
        {
            if (!IsStopped())
            {
                bool newItemsBeforeTimeout = true;
                while (newItemsBeforeTimeout)
                {
                    if (this.fetchQueue.TryDequeue(out string returnInstanceId))
                    {
                        try
                        {
                            return await RetryHelper.ExecuteWithRetryOnTransient(async () =>
                            {
                                using (var txn = this.StateManager.CreateTransaction())
                                {
                                    var existingValue = await this.Store.TryGetValueAsync(txn, returnInstanceId);

                                    if (existingValue.HasValue)
                                    {
                                        if (this.lockedSessions.TryUpdate(returnInstanceId, newValue: LockState.Locked, comparisonValue: LockState.InFetchQueue))
                                        {
                                            ServiceFabricProviderEventSource.Tracing.TraceMessage(returnInstanceId, "Session Locked Accepted");
                                            return existingValue.Value;
                                        }
                                        else
                                        {
                                            var errorMessage = $"Internal Server Error : Unexpected to dequeue the session {returnInstanceId} which was already locked before";
                                            ServiceFabricProviderEventSource.Tracing.UnexpectedCodeCondition(errorMessage);
                                            throw new Exception(errorMessage);
                                        }
                                    }
                                    else
                                    {
                                        var errorMessage = $"Internal Server Error: Did not find the session object in reliable dictionary while having the session {returnInstanceId} in memory";
                                        ServiceFabricProviderEventSource.Tracing.UnexpectedCodeCondition(errorMessage);
                                        throw new Exception(errorMessage);
                                    }
                                }
                            }, uniqueActionIdentifier: $"{nameof(SessionProvider)}.{nameof(AcceptSessionAsync)}, SessionId : {returnInstanceId}");
                        }
                        catch (Exception)
                        {
                            this.fetchQueue.Enqueue(returnInstanceId);
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

        public async Task<List<Message<Guid, TaskMessageItem>>> ReceiveSessionMessagesAsync(PersistentSession session)
        {
            var sessionMessageProvider = await GetOrAddSessionMessagesInstance(session.SessionId);
            var messages = await sessionMessageProvider.ReceiveBatchAsync();
            ServiceFabricProviderEventSource.Tracing.TraceMessage(session.SessionId.InstanceId, $"Number of received messages {messages.Count}");
            return messages;
        }

        public async Task CompleteMessages(ITransaction transaction, OrchestrationInstance instance, List<Guid> lockTokens)
        {
            if (this.sessionMessageProviders.TryGetValue(instance, out SessionMessageProvider sessionMessageProvider))
            {
                ServiceFabricProviderEventSource.Tracing.TraceMessage(instance.InstanceId, $"Number of completed messages {lockTokens.Count}");
                await sessionMessageProvider.CompleteBatchAsync(transaction, lockTokens);
            }
            else
            {
                ServiceFabricProviderEventSource.Tracing.UnexpectedCodeCondition($"{nameof(SessionProvider)}.{nameof(CompleteMessages)} : Did not find session messages provider instance for session : {instance}.");
            }
        }

        public async Task UpdateSessionState(ITransaction transaction, OrchestrationInstance instance, OrchestrationRuntimeState newSessionState)
        {
            var sessionStateEvents = newSessionState?.Events.ToImmutableList();
            var result = PersistentSession.Create(instance, sessionStateEvents);
            await this.Store.SetAsync(transaction, instance.InstanceId, result);
        }

        public async Task AppendMessageAsync(TaskMessageItem newMessage)
        {
            await RetryHelper.ExecuteWithRetryOnTransient(async () =>
            {
                using (var txn = this.StateManager.CreateTransaction())
                {
                    await this.AppendMessageAsync(txn, newMessage);
                    await txn.CommitAsync();
                }
            }, uniqueActionIdentifier: $"Orchestration = '{newMessage.TaskMessage.OrchestrationInstance}', Action = '{nameof(SessionProvider)}.{nameof(AppendMessageAsync)}'");

            this.TryEnqueueSession(newMessage.TaskMessage.OrchestrationInstance);
        }

        public async Task AppendMessageAsync(ITransaction transaction, TaskMessageItem newMessage)
        {
            await EnsureStoreInitialized();
            var sessionMessageProvider = await GetOrAddSessionMessagesInstance(newMessage.TaskMessage.OrchestrationInstance);
            await sessionMessageProvider.SendBeginAsync(transaction, new Message<Guid, TaskMessageItem>(Guid.NewGuid(), newMessage));
            await this.Store.TryAddAsync(transaction, newMessage.TaskMessage.OrchestrationInstance.InstanceId, PersistentSession.Create(newMessage.TaskMessage.OrchestrationInstance));
        }

        public async Task<bool> TryAppendMessageAsync(ITransaction transaction, TaskMessageItem newMessage)
        {
            if (await this.Store.ContainsKeyAsync(transaction, newMessage.TaskMessage.OrchestrationInstance.InstanceId))
            {
                var sessionMessageProvider = await GetOrAddSessionMessagesInstance(newMessage.TaskMessage.OrchestrationInstance);
                await sessionMessageProvider.SendBeginAsync(transaction, new Message<Guid, TaskMessageItem>(Guid.NewGuid(), newMessage));
                return true;
            }

            return false;
        }

        public async Task<IList<OrchestrationInstance>> TryAppendMessageBatchAsync(ITransaction transaction, IEnumerable<TaskMessageItem> newMessages)
        {
            List<OrchestrationInstance> modifiedSessions = new List<OrchestrationInstance>();

            var groups = newMessages.GroupBy(m => m.TaskMessage.OrchestrationInstance, OrchestrationInstanceComparer.Default);

            foreach (var group in groups)
            {
                if (await this.Store.ContainsKeyAsync(transaction, group.Key.InstanceId))
                {
                    var sessionMessageProvider = await GetOrAddSessionMessagesInstance(group.Key);
                    await sessionMessageProvider.SendBatchBeginAsync(transaction, group.Select(tm => new Message<Guid, TaskMessageItem>(Guid.NewGuid(), tm)));
                    modifiedSessions.Add(group.Key);
                }
            }

            return modifiedSessions;
        }

        public async Task AppendMessageBatchAsync(ITransaction transaction, IEnumerable<TaskMessageItem> newMessages)
        {
            var groups = newMessages.GroupBy(m => m.TaskMessage.OrchestrationInstance, OrchestrationInstanceComparer.Default);

            foreach (var group in groups)
            {
                var groupMessages = group.AsEnumerable();

                await this.Store.TryAddAsync(transaction, group.Key.InstanceId, PersistentSession.Create(group.Key));
                var sessionMessageProvider = await GetOrAddSessionMessagesInstance(group.Key);
                await sessionMessageProvider.SendBatchBeginAsync(transaction, group.Select(tm => new Message<Guid, TaskMessageItem>(Guid.NewGuid(), tm)));
            }
        }

        public async Task<IEnumerable<PersistentSession>> GetSessions()
        {
            await EnsureStoreInitialized();

            var result = new List<PersistentSession>();
            await this.EnumerateItems(kvp => result.Add(kvp.Value));
            return result;
        }

        public void TryUnlockSession(OrchestrationInstance instance, bool abandon = false, bool isComplete = false)
        {
            ServiceFabricProviderEventSource.Tracing.TraceMessage(instance.InstanceId, $"Session Unlock Begin, Abandon = {abandon}");
            if (!this.lockedSessions.TryRemove(instance.InstanceId, out LockState lockState) || lockState == LockState.InFetchQueue)
            {
                var errorMessage = $"{nameof(SessionProvider)}.{nameof(TryUnlockSession)} : Trying to unlock the session {instance.InstanceId} which was not locked.";
                ServiceFabricProviderEventSource.Tracing.UnexpectedCodeCondition(errorMessage);
                throw new Exception(errorMessage);
            }

            if (!isComplete && (abandon || lockState == LockState.NewMessagesWhileLocked))
            {
                this.TryEnqueueSession(instance);
            }

            ServiceFabricProviderEventSource.Tracing.TraceMessage(instance.InstanceId, $"Session Unlock End, Abandon = {abandon}, removed lock state = {lockState}");
        }

        public async Task<bool> TryAddSession(ITransaction transaction, TaskMessageItem newMessage)
        {
            await EnsureStoreInitialized();

            bool added = await this.Store.TryAddAsync(transaction, newMessage.TaskMessage.OrchestrationInstance.InstanceId, PersistentSession.Create(newMessage.TaskMessage.OrchestrationInstance));

            if (added)
            {
                var sessionMessageProvider = await GetOrAddSessionMessagesInstance(newMessage.TaskMessage.OrchestrationInstance);
                await sessionMessageProvider.SendBeginAsync(transaction, new Message<Guid, TaskMessageItem>(Guid.NewGuid(), newMessage));
            }

            return added;
        }

        public async Task<PersistentSession> GetSession(string instanceId)
        {
            await EnsureStoreInitialized();

            return await RetryHelper.ExecuteWithRetryOnTransient(async () =>
            {
                using (var txn = this.StateManager.CreateTransaction())
                {
                    var value = await this.Store.TryGetValueAsync(txn, instanceId);
                    if (value.HasValue)
                    {
                        return value.Value;
                    }
                }
                return null;
            }, uniqueActionIdentifier: $"Orchestration InstanceId = {instanceId}, Action = {nameof(SessionProvider)}.{nameof(GetSession)}");
        }

        public void TryEnqueueSession(OrchestrationInstance instance)
        {
            this.TryEnqueueSession(instance.InstanceId);
        }

        void TryEnqueueSession(string instanceId)
        {
            if (this.lockedSessions.TryAdd(instanceId, LockState.InFetchQueue))
            {
                ServiceFabricProviderEventSource.Tracing.TraceMessage(instanceId, "Session Getting Enqueued");
                this.fetchQueue.Enqueue(instanceId);
                SetWaiterForNewItems();
            }
            else
            {
                this.lockedSessions.TryUpdate(instanceId, LockState.NewMessagesWhileLocked, LockState.Locked);
            }
        }

        public async Task DropSession(ITransaction txn, OrchestrationInstance instance)
        {
            if (instance == null)
            {
                throw new ArgumentNullException(nameof(instance));
            }

            await this.Store.TryRemoveAsync(txn, instance.InstanceId);

            this.sessionMessageProviders.TryRemove(instance, out SessionMessageProvider _);

            var noWait = RetryHelper.ExecuteWithRetryOnTransient(() => this.StateManager.RemoveAsync(GetSessionMessagesDictionaryName(instance)),
                uniqueActionIdentifier: $"Orchestration = '{instance}', Action = 'DropSessionMessagesDictionaryBackgroundTask'");
        }

        async Task<SessionMessageProvider> GetOrAddSessionMessagesInstance(OrchestrationInstance instance)
        {
            var newInstance = new SessionMessageProvider(this.StateManager, GetSessionMessagesDictionaryName(instance), this.CancellationToken);
            var sessionMessageProvider = this.sessionMessageProviders.GetOrAdd(instance, newInstance);
            await sessionMessageProvider.EnsureStoreInitialized();
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
