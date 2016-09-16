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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Data;
    using Microsoft.ServiceFabric.Data.Collections;

    class SessionsProvider
    {
        IReliableStateManager stateManager;
        IReliableDictionary<string, PersistentSession> orchestrations;
        CancellationTokenSource cancellationTokenSource;

        readonly Func<string, PersistentSession> NewSessionFactory = (sId) => PersistentSession.Create(sId);

        public SessionsProvider(IReliableStateManager stateManager, IReliableDictionary<string, PersistentSession> orchestrations)
        {
            if (stateManager == null)
            {
                throw new ArgumentNullException(nameof(stateManager));
            }

            if (orchestrations == null)
            {
                throw new ArgumentNullException(nameof(orchestrations));
            }

            this.stateManager = stateManager;
            this.orchestrations = orchestrations;
            this.cancellationTokenSource = new CancellationTokenSource();
        }

        public Task StartAsync()
        {
            var nowait = Task.Run(async () =>
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
            });

            return Task.FromResult<object>(null);
        }

        public void Stop()
        {
            this.cancellationTokenSource.Cancel();
        }

        //Todo: This is O(N) and also a frequent operation, do we need to optimize this?
        public async Task<PersistentSession> AcceptSessionAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            Stopwatch timer = Stopwatch.StartNew();
            while (timer.Elapsed < receiveTimeout && !cancellationToken.IsCancellationRequested)
            {
                string returnSessionId = null;
                using (var tx = this.stateManager.CreateTransaction())
                {
                    var enumerable = await this.orchestrations.CreateEnumerableAsync(tx, EnumerationMode.Unordered);
                    using (var enumerator = enumerable.GetAsyncEnumerator())
                    {
                        while (await enumerator.MoveNextAsync(cancellationToken))
                        {
                            var entry = enumerator.Current;

                            if (!entry.Value.Messages.Any())
                            {
                                continue;
                            }

                            returnSessionId = entry.Key;
                            break;
                        }
                    }
                }

                if (returnSessionId != null)
                {
                    using (var txn = this.stateManager.CreateTransaction())
                    {
                        //Todo: TryUpdate feels more natural here than AddOrUpdateAsync, however what do we do if it fails?
                        var result = await this.orchestrations.AddOrUpdateAsync(txn, returnSessionId, NewSessionFactory, (sId, oldValue) => oldValue.ReceiveMessages());
                        await txn.CommitAsync();
                        return result;
                    }
                }

                await Task.Delay(100, cancellationToken);
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
    }
}
