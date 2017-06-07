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
    using System.Threading;
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

        public ScheduledMessageProvider(IReliableStateManager stateManager, string storeName, SessionsProvider sessionsProvider, CancellationToken token) : base(stateManager, storeName, token)
        {
            this.sessionsProvider = sessionsProvider;
        }

        public override async Task StartAsync()
        {
            await InitializeStore();

            var builder = this.inMemorySet.ToBuilder();

            await this.EnumerateItems(kvp =>
            {
                var timerEvent = kvp.Value?.Event as TimerFiredEvent;
                if (timerEvent == null)
                {
                    ProviderEventSource.Log.UnexpectedCodeCondition($"{nameof(ScheduledMessageProvider)}.{nameof(StartAsync)} : Seeing a non timer event in scheduled messages while filling the pending items collection in role start");
                }
                else
                {
                    builder.Add(new Message<string, TaskMessage>(kvp.Key, kvp.Value));
                }
            });

            lock (@lock)
            {
                if (this.inMemorySet.Count > 0)
                {
                    ProviderEventSource.Log.UnexpectedCodeCondition($"{nameof(ScheduledMessageProvider)}.{nameof(StartAsync)} : Before we set the In memory set from the builder, there are items in it which should not happen.");
                }
                this.inMemorySet = builder.ToImmutableSortedSet(TimerFiredEventComparer.Instance);
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

            var timerEvent = value.Event as TimerFiredEvent;
            if (timerEvent != null && timerEvent.FireAt < this.nextActivationCheck)
            {
                SetWaiterForNewItems();
            }
        }

        // Since this method is started as part of StartAsync, the other stores maynot be immediately initialized
        // by the time this invokes operations on those stores. But that would be a transient error and the next
        // iteration of processing should take care of making things right.
        async Task ProcessScheduledMessages()
        {
            int successiveFailureCount = 0;
            while (!IsStopped())
            {
                try
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

                        IList<OrchestrationInstance> modifiedSessions = null;

                        await RetryHelper.ExecuteWithRetryOnTransient(async () =>
                        {
                            using (var tx = this.StateManager.CreateTransaction())
                            {
                                modifiedSessions = await this.sessionsProvider.TryAppendMessageBatchAsync(tx, values);
                                await this.CompleteBatchAsync(tx, keys);
                                await tx.CommitAsync();
                            }
                        }, uniqueActionIdentifier: $"Action = '{nameof(ScheduledMessageProvider)}.{nameof(ProcessScheduledMessages)}'");

                        lock (@lock)
                        {
                            this.inMemorySet = this.inMemorySet.Except(activatedMessages);
                        }

                        if (modifiedSessions != null)
                        {
                            foreach (var sessionId in modifiedSessions)
                            {
                                this.sessionsProvider.TryEnqueueSession(sessionId);
                            }
                        }
                    }

                    this.nextActivationCheck = nextCheck;
                    await WaitForItemsAsync(this.nextActivationCheck - DateTime.UtcNow);
                    successiveFailureCount = 0;
                }
                catch (Exception e)
                {
                    successiveFailureCount++;
                    ProviderEventSource.Log.ExceptionWhileProcessingScheduledMessages($"{nameof(ScheduledMessageProvider)}.{nameof(ProcessScheduledMessages)}", successiveFailureCount, e.Message, e.StackTrace);
                    await Task.Delay(TimeSpan.FromMilliseconds(100));
                }
            }
        }
    }
}
