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
    using System.Linq;
    using System.Threading.Tasks;
    using DurableTask.History;
    using Microsoft.ServiceFabric.Data;
    using Microsoft.ServiceFabric.Data.Collections;

    class ScheduledMessageProvider : MessageProviderBase<string, TaskMessage>
    {
        readonly SessionsProvider sessionsProvider;
        readonly object @lock = new object();

        ImmutableSortedSet<Message<string, TaskMessage>> inMemorySet = ImmutableSortedSet<Message<string, TaskMessage>>.Empty.WithComparer(TimerFiredEventComparer.Instance);
        DateTime nextActivationCheck;

        public ScheduledMessageProvider(IReliableStateManager stateManager, string storeName, SessionsProvider sessionsProvider) : base(stateManager, storeName)
        {
            this.sessionsProvider = sessionsProvider;
        }

        public override async Task StartAsync()
        {
            await InitializeStore();

            using (var tx = this.StateManager.CreateTransaction())
            {
                var count = await this.Store.GetCountAsync(tx);

                if (count > 0)
                {
                    var builder = this.inMemorySet.ToBuilder();

                    var enumerable = await this.Store.CreateEnumerableAsync(tx, EnumerationMode.Unordered);
                    using (var enumerator = enumerable.GetAsyncEnumerator())
                    {
                        while (await enumerator.MoveNextAsync(this.CancellationToken))
                        {
                            var timerEvent = enumerator.Current.Value?.Event as TimerFiredEvent;
                            if (timerEvent == null)
                            {
                                ProviderEventSource.Instance.LogUnexpectedCodeCondition($"{nameof(ScheduledMessageProvider)}.{nameof(StartAsync)} : Seeing a non timer event in scheduled messages while filling the pending items collection in role start");
                            }
                            else
                            {
                                builder.Add(new Message<string, TaskMessage>(enumerator.Current.Key, enumerator.Current.Value));
                            }
                        }
                    }

                    lock (@lock)
                    {
                        this.inMemorySet = builder.ToImmutableSortedSet();
                    }
                }
            }

            var metricsTask = LogMetrics();
            var nowait = ProcessScheduledMessages();
        }

        protected override void AddItemInMemory(string key, TaskMessage value)
        {
            lock (@lock)
            {
                this.inMemorySet = this.inMemorySet.Add(new Message<string, TaskMessage>(key, value));
            }

            //Todo: Do i need to make nextActivationCheck modifications thread safe?
            var timerEvent = value.Event as TimerFiredEvent;
            if (timerEvent != null && timerEvent.FireAt < this.nextActivationCheck)
            {
                SetWaiterForNewItems();
            }
        }

        async Task ProcessScheduledMessages()
        {
            while (!IsStopped())
            {
                var currentTime = DateTime.UtcNow;
                var nextCheck = currentTime + TimeSpan.FromSeconds(1);

                var builder = this.inMemorySet.ToBuilder();
                List<Message<string, TaskMessage>> activatedMessages = new List<Message<string, TaskMessage>>();

                while (builder.Count > 0)
                {
                    var firstPendingMessage = builder.Min;
                    var timerEvent = firstPendingMessage.Value.Event as TimerFiredEvent;

                    if (timerEvent == null)
                    {
                        throw new Exception("Internal Server Error : Ended up adding non TimerFiredEvent TaskMessage as scheduled message");
                    }

                    if (timerEvent.FireAt <= currentTime)
                    {
                        activatedMessages.Add(firstPendingMessage);
                        builder.Remove(firstPendingMessage);
                    }
                    else
                    {
                        nextCheck = timerEvent.FireAt;
                        break;
                    }
                }

                if (IsStopped())
                {
                    break;
                }

                if (activatedMessages.Count > 0)
                {
                    var keys = activatedMessages.Select(m => m.Key);
                    var values = activatedMessages.Select(m => m.Value).ToList();

                    using (var tx = this.StateManager.CreateTransaction())
                    {
                        var modifiedSessions = await this.sessionsProvider.TryAppendMessageBatchAsync(tx, values);
                        await this.CompleteBatchAsync(tx, keys);
                        await tx.CommitAsync();

                        lock (@lock)
                        {
                            this.inMemorySet = this.inMemorySet.Except(activatedMessages);
                        }

                        foreach (var sessionId in modifiedSessions)
                        {
                            this.sessionsProvider.TryEnqueueSession(sessionId);
                        }
                    }
                }

                this.nextActivationCheck = nextCheck;
                await WaitForItemsAsync(this.nextActivationCheck - DateTime.UtcNow);
            }
        }
    }
}
