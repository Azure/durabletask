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
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Data;
    using Microsoft.ServiceFabric.Data.Collections;

    public class SessionsProvider
    {
        readonly IReliableStateManager stateManager;
        readonly CancellationTokenSource cancellationTokenSource;

        readonly Func<string, PersistentSession> NewSessionFactory = (sId) => PersistentSession.Create(sId, null, null);
        IReliableDictionary<string, PersistentSession> orchestrations;

        ConcurrentQueue<string> inMemorySessionsQueue = new ConcurrentQueue<string>();
        ConcurrentDictionary<string, LockState> lockedSessions = new ConcurrentDictionary<string, LockState>();

        readonly AsyncManualResetEvent waitEvent = new AsyncManualResetEvent();

        public SessionsProvider(IReliableStateManager stateManager)
        {
            if (stateManager == null)
            {
                throw new ArgumentNullException(nameof(stateManager));
            }

            this.stateManager = stateManager;
            this.cancellationTokenSource = new CancellationTokenSource();
        }

        public async Task StartAsync()
        {
            this.orchestrations = await this.stateManager.GetOrAddAsync<IReliableDictionary<string, PersistentSession>>(Constants.OrchestrationDictionaryName);

            using (var txn = this.stateManager.CreateTransaction())
            {
                var count = await this.orchestrations.GetCountAsync(txn);

                if (count > 0)
                {
                    var enumerable = await this.orchestrations.CreateEnumerableAsync(txn, EnumerationMode.Unordered);
                    using (var enumerator = enumerable.GetAsyncEnumerator())
                    {
                        while (await enumerator.MoveNextAsync(this.cancellationTokenSource.Token))
                        {
                            var entry = enumerator.Current;
                            if (entry.Value.Messages.Any())
                            {
                                this.TryEnqueueSession(entry.Key);
                            }
                        }
                    }
                }
            }
        }

        public Task StopAsync()
        {
            this.cancellationTokenSource.Cancel();
            return Task.FromResult<object>(null);
        }

        public async Task<PersistentSession> AcceptSessionAsync(TimeSpan receiveTimeout)
        {
            ThrowIfStopped();

            string returnSessionId;
            bool newItemsBeforeTimeout = true;
            while (newItemsBeforeTimeout)
            {

                if (this.inMemorySessionsQueue.TryDequeue(out returnSessionId))
                {
                    try
                    {
                        using (var txn = this.stateManager.CreateTransaction())
                        {
                            //Todo: TryUpdate feels more natural here than AddOrUpdateAsync, however what do we do if it fails?
                            var result = await this.orchestrations.AddOrUpdateAsync(txn, returnSessionId, NewSessionFactory, (sId, oldValue) => oldValue.ReceiveMessages());
                            await txn.CommitAsync();

                            if (!this.lockedSessions.TryUpdate(returnSessionId, newValue:LockState.Locked, comparisonValue:LockState.InFetchQueue))
                            {
                                throw new Exception("Internal Server Error : Unexpected to dequeue a session which was already locked before");
                            }

                            return result;
                        }
                    }
                    catch (TimeoutException)
                    {
                        this.inMemorySessionsQueue.Enqueue(returnSessionId);
                        throw;
                    }
                }

                this.waitEvent.Reset();
                newItemsBeforeTimeout = await this.waitEvent.WaitAsync(receiveTimeout, this.cancellationTokenSource.Token);
            }

            return null;
        }

        public List<TaskMessage> GetSessionMessages(PersistentSession session)
        {
            return session.Messages.Where(m => m.IsReceived).Select(m => m.TaskMessage).ToList();
        }

        /// <summary>
        /// Callers should pass the transaction and once the transaction is commited successfully,
        /// should call <see cref="DurableTask.ServiceFabric.SessionsProvider.TryUnlockSession"/>.
        /// </summary>
        public Task<PersistentSession> CompleteAndUpdateSession(ITransaction transaction,
            string sessionId,
            OrchestrationRuntimeState newSessionState)
        {
            return this.orchestrations.AddOrUpdateAsync(transaction, sessionId, NewSessionFactory,
                (sId, oldValue) => oldValue.CompleteMessages(newSessionState));
        }

        public async Task AppendMessageAsync(TaskMessage newMessage)
        {
            ThrowIfStopped();
            await EnsureOrchestrationStoreInitialized();

            using (var txn = this.stateManager.CreateTransaction())
            {
                await this.AppendMessageAsync(txn, newMessage);
                await txn.CommitAsync();
            }

            this.TryEnqueueSession(newMessage.OrchestrationInstance.InstanceId);
        }

        public async Task AppendMessageAsync(ITransaction transaction, TaskMessage newMessage)
        {
            ThrowIfStopped();
            Func<string, PersistentSession> newSessionFactory = (sId) => PersistentSession.CreateWithNewMessage(sId, newMessage);

            await this.orchestrations.AddOrUpdateAsync(transaction, newMessage.OrchestrationInstance.InstanceId,
                addValueFactory: newSessionFactory,
                updateValueFactory: (ses, oldValue) => oldValue.AppendMessage(newMessage));
        }

        public async Task<bool> TryAppendMessageAsync(ITransaction transaction, TaskMessage newMessage)
        {
            ThrowIfStopped();

            var sessionId = newMessage.OrchestrationInstance.InstanceId;
            var existingValue = await this.orchestrations.TryGetValueAsync(transaction, sessionId, LockMode.Update);
            if (existingValue.HasValue)
            {
                var newValue = existingValue.Value.AppendMessage(newMessage);
                if (newValue != existingValue.Value)
                {
                    await this.orchestrations.TryUpdateAsync(transaction, sessionId, newValue, existingValue.Value);
                    return true;
                }
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
                var existingValue = await this.orchestrations.TryGetValueAsync(transaction, group.Key, LockMode.Update);
                if (existingValue.HasValue)
                {
                    var newValue = existingValue.Value.AppendMessageBatch(group.AsEnumerable());
                    if (newValue != existingValue.Value)
                    {
                        await this.orchestrations.TryUpdateAsync(transaction, group.Key, newValue, existingValue.Value);
                        modifiedSessions.Add(group.Key);
                    }
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

                Func<string, PersistentSession> newSessionFactory = (sId) => PersistentSession.CreateWithNewMessages(sId, groupMessages);

                await this.orchestrations.AddOrUpdateAsync(transaction, group.Key,
                    addValueFactory: newSessionFactory,
                    updateValueFactory: (ses, oldValue) => oldValue.AppendMessageBatch(groupMessages));
            }
        }

        public void TryUnlockSession(string sessionId, bool putBackInQueue)
        {
            LockState lockState;
            if (!this.lockedSessions.TryRemove(sessionId, out lockState) || lockState != LockState.Locked)
            {
                throw new Exception("Internal Server Error : Unexpectedly trying to unlock a session which was not locked.");
            }

            if (putBackInQueue)
            {
                TryEnqueueSession(sessionId);
            }
        }

        public async Task<bool> SessionExists(string sessionId)
        {
            await EnsureOrchestrationStoreInitialized();

            using (var txn = this.stateManager.CreateTransaction())
            {
                return await this.orchestrations.ContainsKeyAsync(txn, sessionId);
            }
        }

        async Task EnsureOrchestrationStoreInitialized()
        {
            //Workaround to avoid client sending a new message before StartAsync on service is done
            if (this.orchestrations == null)
            {
                this.orchestrations = await this.stateManager.GetOrAddAsync<IReliableDictionary<string, PersistentSession>>(Constants.OrchestrationDictionaryName);
            }
        }

        public void TryEnqueueSession(string sessionId)
        {
            if (this.lockedSessions.TryAdd(sessionId, LockState.InFetchQueue))
            {
                this.inMemorySessionsQueue.Enqueue(sessionId);
                this.waitEvent.Set();
            }
        }

        public async Task DropSession(ITransaction transaction, string sessionId)
        {
            await this.orchestrations.TryRemoveAsync(transaction, sessionId);
        }

        void ThrowIfStopped()
        {
            this.cancellationTokenSource.Token.ThrowIfCancellationRequested();
        }

        enum LockState
        {
            InFetchQueue = 0,

            Locked,
        }
    }
}
