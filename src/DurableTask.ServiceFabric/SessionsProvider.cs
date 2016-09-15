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

        readonly Func<string, PersistentSession> NewSessionFactory = (sId) => PersistentSession.Create(sId, null, null, null, false);
        IReliableDictionary<string, PersistentSession> orchestrations;
        ConcurrentQueue<string> inMemorySessions = new ConcurrentQueue<string>();

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
            var nowait = ProcessScheduledMessages();
        }

        async Task ProcessScheduledMessages()
        {
            while (!this.cancellationTokenSource.IsCancellationRequested)
            {
                var scheduledMessageSessions = new List<string>();
                using (var txn = this.stateManager.CreateTransaction())
                {
                    var enumerable = await this.orchestrations.CreateEnumerableAsync(txn, EnumerationMode.Unordered);
                    using (var enumerator = enumerable.GetAsyncEnumerator())
                    {
                        while (await enumerator.MoveNextAsync(this.cancellationTokenSource.Token))
                        {
                            var entry = enumerator.Current;
                            if (entry.Value.ScheduledMessages.Any())
                            {
                                scheduledMessageSessions.Add(entry.Key);
                            }
                        }
                    }
                }

                foreach (var sessionId in scheduledMessageSessions)
                {
                    using (var txn = this.stateManager.CreateTransaction())
                    {
                        await this.orchestrations.AddOrUpdateAsync(txn, sessionId, NewSessionFactory, (sId, oldValue) => oldValue.FireScheduledMessages());
                        await txn.CommitAsync();
                    }
                }

                await Task.Delay(TimeSpan.FromMilliseconds(100));
            }
        }

        public void Stop()
        {
            this.cancellationTokenSource.Cancel();
        }

        public async Task<PersistentSession> AcceptSessionAsync(TimeSpan receiveTimeout)
        {
            Stopwatch timer = Stopwatch.StartNew();
            while (timer.Elapsed < receiveTimeout && !this.cancellationTokenSource.IsCancellationRequested)
            {
                string returnSessionId;

                if (this.inMemorySessions.TryDequeue(out returnSessionId))
                {
                    using (var txn = this.stateManager.CreateTransaction())
                    {
                        //Todo: TryUpdate feels more natural here than AddOrUpdateAsync, however what do we do if it fails?
                        var result = await this.orchestrations.AddOrUpdateAsync(txn, returnSessionId, NewSessionFactory, (sId, oldValue) => oldValue.ReceiveMessages());
                        await txn.CommitAsync();
                        return result;
                    }
                }

                await Task.Delay(100, this.cancellationTokenSource.Token);
                await PopulateInMemorySessions();
            }

            return null;
        }

        public List<TaskMessage> GetSessionMessages(PersistentSession session)
        {
            return session.Messages.Where(m => m.IsReceived).Select(m => m.TaskMessage).ToList();
        }

        public async Task CompleteAndUpdateSession(ITransaction transaction,
            string sessionId,
            OrchestrationRuntimeState newSessionState,
            IList<TaskMessage> scheduledMessages)
        {
            await this.orchestrations.AddOrUpdateAsync(transaction, sessionId, NewSessionFactory,
                (sId, oldValue) => oldValue.CompleteMessages(newSessionState, scheduledMessages));
        }

        public async Task AppendMessageAsync(ITransaction transaction, TaskMessage newMessage)
        {
            //Workaround to avoid client sending a new message before StartAsync on service is done
            if (this.orchestrations == null)
            {
                this.orchestrations = await this.stateManager.GetOrAddAsync<IReliableDictionary<string, PersistentSession>>(Constants.OrchestrationDictionaryName);
            }

            Func<string, PersistentSession> newSessionFactory = (sId) => PersistentSession.CreateWithNewMessage(sId, newMessage);

            await this.orchestrations.AddOrUpdateAsync(transaction, newMessage.OrchestrationInstance.InstanceId,
                addValueFactory: newSessionFactory,
                updateValueFactory: (ses, oldValue) => oldValue.AppendMessage(newMessage));
        }

        public async Task AppendMessageBatchAsync(ITransaction transaction, IEnumerable<TaskMessage> newMessages)
        {
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

        public async Task ReleaseSession(ITransaction transaction, string sessionId)
        {
            await this.orchestrations.TryRemoveAsync(transaction, sessionId);
        }

        async Task PopulateInMemorySessions()
        {
            if (!this.cancellationTokenSource.IsCancellationRequested)
            {
                using (var txn = this.stateManager.CreateTransaction())
                {
                    var enumerable = await this.orchestrations.CreateEnumerableAsync(txn, EnumerationMode.Unordered);
                    using (var enumerator = enumerable.GetAsyncEnumerator())
                    {
                        while (await enumerator.MoveNextAsync(this.cancellationTokenSource.Token))
                        {
                            var entry = enumerator.Current;
                            if (!entry.Value.IsLocked && entry.Value.Messages.Any())
                            {
                                this.inMemorySessions.Enqueue(entry.Key);
                            }
                        }
                    }
                }
            }
        }
    }
}
