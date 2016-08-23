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
    using System.Collections.Immutable;
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
                    await Task.Delay(TimeSpan.FromMilliseconds(100));
                    using (var txn = this.stateManager.CreateTransaction())
                    {
                        var enumerable = await this.orchestrations.CreateEnumerableAsync(txn, EnumerationMode.Unordered);
                        using (var enumerator = enumerable.GetAsyncEnumerator())
                        {
                            while (await enumerator.MoveNextAsync(this.cancellationTokenSource.Token))
                            {
                                var entry = enumerator.Current;
                                PersistentSession newValue;
                                if (entry.Value.CheckScheduledMessages(out newValue))
                                {
                                    await this.orchestrations.SetAsync(txn, entry.Key, newValue); //Is this valid code? Can enumeration update the value?
                                }
                            }
                        }
                        await txn.CommitAsync();
                    }
                }
            });

            return Task.FromResult<object>(null);
        }

        public void Stop()
        {
            this.cancellationTokenSource.Cancel();
        }

        //Todo: This is O(N) and also a frequent operation, do we need to optimize this?
        //Todo: Should this use the same transation as complete??
        public async Task<PersistentSession> AcceptSessionAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            Stopwatch timer = Stopwatch.StartNew();
            while (timer.Elapsed < receiveTimeout && !cancellationToken.IsCancellationRequested)
            {
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

                            return entry.Value;
                        }
                    }
                }
                await Task.Delay(100, cancellationToken);
            }

            return null;
        }

        public List<TaskMessage> GetSessionMessages(PersistentSession session)
        {
            return session.ReceiveMessages();
        }

        public async Task CompleteAndUpdateSession(ITransaction transaction,
            PersistentSession session,
            OrchestrationRuntimeState newSessionState,
            IList<TaskMessage> scheduledMessages)
        {
            var newSession = session
                .CompleteMessages()
                .SetSessionState(newSessionState);

            if (scheduledMessages?.Count > 0)
            {
                newSession = newSession.AppendScheduledMessagesBatch(scheduledMessages);
            }

            await this.orchestrations.SetAsync(transaction, session.SessionId, newSession);
        }

        public async Task AppendMessageAsync(ITransaction transaction, string sessionId, TaskMessage newMessage)
        {
            Func<string, PersistentSession> newSessionFactory = (sId) => new PersistentSession(sId, new OrchestrationRuntimeState(),
                ImmutableList<LockableTaskMessage>.Empty.Add(new LockableTaskMessage() {TaskMessage = newMessage}), null);

            await this.orchestrations.AddOrUpdateAsync(transaction, sessionId,
                addValueFactory: newSessionFactory,
                updateValueFactory: (ses, oldValue) => oldValue.AppendMessage(newMessage));
        }
    }
}
