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

    public class SessionsProvider
    {
        IReliableStateManager stateManager;
        IReliableDictionary<string, PersistentSession> orchestrations;

        public SessionsProvider(IReliableDictionary<string, PersistentSession> orchestrations, IReliableStateManager stateManager)
        {
            if (orchestrations == null)
            {
                throw new ArgumentNullException(nameof(orchestrations));
            }

            if (stateManager == null)
            {
                throw new ArgumentNullException(nameof(stateManager));
            }

            this.orchestrations = orchestrations;
            this.stateManager = stateManager;
        }

        //Todo: This is O(N) and also a frequent operation, do we need to optimize this?
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

        public async Task CompleteSessionMessages(ITransaction transaction, PersistentSession session)
        {
            var newSession = session.CompleteMessages();
            await this.orchestrations.SetAsync(transaction, session.SessionId, newSession);
        }

        public async Task AppendMessageAsync(ITransaction transaction, string sessionId, TaskMessage newMessage)
        {
            var newSession = new PersistentSession(sessionId, new OrchestrationRuntimeState(),
                ImmutableList<LockableTaskMessage>.Empty.Add(new LockableTaskMessage() {TaskMessage = newMessage}));

            await this.orchestrations.AddOrUpdateAsync(transaction, sessionId,
                addValue: newSession,
                updateValueFactory: (ses, oldValue) => oldValue.AppendMessage(newMessage));
        }
    }
}
