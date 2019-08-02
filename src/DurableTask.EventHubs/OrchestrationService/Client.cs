using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.EventHubs
{
    internal class Client : Backend.IClient
    {
        private readonly CancellationToken shutdownToken; 
        private static TimeSpan DefaultTimeout = TimeSpan.FromMinutes(5);

        public Guid ClientId { get; private set; }
        private Backend.ISender BatchSender { get; set; }


        private long SequenceNumber; // for numbering requests that enter on this client

        private BatchTimer<ResponseWaiter> ResponseTimeouts;
        private ConcurrentDictionary<long, ResponseWaiter> ResponseWaiters;
        private Dictionary<Guid, List<ClientEventFragment>> Fragments;

        public string AbbreviatedClientId; // used for tracing
        
        public Client(Guid clientId, Backend.ISender batchSender, CancellationToken shutdownToken)
        {
            this.ClientId = clientId;
            this.AbbreviatedClientId = clientId.ToString("N").Substring(0,7);
            this.BatchSender = batchSender;
            this.shutdownToken = shutdownToken;
            this.ResponseTimeouts = new BatchTimer<ResponseWaiter>(this.shutdownToken, Timeout);
            this.ResponseWaiters = new ConcurrentDictionary<long, ResponseWaiter>();
            this.Fragments = new Dictionary<Guid, List<ClientEventFragment>>();
        }


        public void Process(IEnumerable<ClientEvent> batch)
        {
            foreach (var clientEvent in batch)
            {
                if (!(clientEvent is ClientEventFragment fragment))
                {
                    TraceReceive(clientEvent);
                    Process(clientEvent);
                }
                else
                {
                    if (!fragment.IsLast)
                    {
                        if (!this.Fragments.TryGetValue(fragment.CohortId, out var list))
                        {
                            this.Fragments[fragment.CohortId] = list = new List<ClientEventFragment>();
                        }
                        list.Add(fragment);
                    }
                    else
                    {
                        var reassembledEvent = (ClientEvent)FragmentationAndReassembly.Reassemble(this.Fragments[fragment.CohortId], fragment);
                        this.Fragments.Remove(fragment.CohortId);

                        TraceReceive(reassembledEvent);
                        Process(reassembledEvent);
                    }
                }
            }
        }

        public void Process(ClientEvent clientEvent)
        {
            if (this.ResponseWaiters.TryGetValue(clientEvent.RequestId, out var waiter))
            {
                waiter.TrySetResult(clientEvent);
            }
        }

        public void Submit(Event evt, Backend.ISendConfirmationListener listener)
        {
            TraceSend(evt);
            this.BatchSender.Submit(evt, listener);
        }

        public void ReportError(string msg, Exception e)
        {
            Trace.TraceError($"Client.{this.AbbreviatedClientId} !!! {msg}: {e}");
        }

        [Conditional("DEBUG")]
        private void TraceSend(Event m)
        {
            Trace.TraceInformation($"Client.{this.AbbreviatedClientId} Sending {m}");
        }

        [Conditional("DEBUG")]
        private void TraceReceive(Event m)
        {
            Trace.TraceInformation($"Client.{this.AbbreviatedClientId} Processing {m}");
        }

        private static void Timeout<T>(IEnumerable<CancellableCompletionSource<T>> promises) where T : class
        {
            foreach (var promise in promises)
            {
                promise.TrySetTimeoutException();
            }
        }

        private Task<ClientEvent> PerformRequestWithTimeoutAndCancellation(CancellationToken token, ClientRequestEvent request, bool doneWhenSent)
        {
            var waiter = new ResponseWaiter(this.shutdownToken, request.RequestId, this);
            this.ResponseWaiters.TryAdd(request.RequestId, waiter);
            this.ResponseTimeouts.Schedule(DateTime.UtcNow + request.Timeout, waiter);

            this.Submit(request, doneWhenSent ? waiter : null);

            return waiter.Task;
        }

        internal class ResponseWaiter : CancellableCompletionSource<ClientEvent>, Backend.ISendConfirmationListener
        {
            private long id;
            private Client client;

            public ResponseWaiter(CancellationToken token, long id, Client client) : base(token)
            {
                this.id = id;
                this.client = client;
            }

            public void ConfirmDurablySent(Event evt)
            {
                this.TrySetResult(null); // task finishes when the send has been confirmed, no result is returned
            }

            public void ReportSenderException(Event evt, Exception e)
            {
                this.TrySetException(e); // task finishes with exception
            }

            protected override void Cleanup()
            {
                client.ResponseWaiters.TryRemove(this.id, out var _);
                base.Cleanup();
            }
        }

        /******************************/
        // orchestration client methods
        /******************************/

        public Task CreateTaskOrchestrationAsync(uint partitionId, TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses)
        {
            var request = new CreationRequestReceived()
            {
                PartitionId = partitionId,
                ClientId = this.ClientId,
                RequestId = Interlocked.Increment(ref this.SequenceNumber),
                TaskMessage = creationMessage,
                DedupeStatuses = dedupeStatuses,
                Timestamp = DateTime.UtcNow,
                Timeout = DefaultTimeout,
            };

            return PerformRequestWithTimeoutAndCancellation(CancellationToken.None, request, false);
        }

        public Task SendTaskOrchestrationMessageBatchAsync(uint partitionId, IEnumerable<TaskMessage> messages)
        {
            var request = new ClientTaskMessagesReceived()
            {
                PartitionId = partitionId,
                ClientId = this.ClientId,
                RequestId = Interlocked.Increment(ref this.SequenceNumber),
                TaskMessages = messages.ToArray(),
                Timeout = DefaultTimeout,
            };

            return PerformRequestWithTimeoutAndCancellation(CancellationToken.None, request, true);
        }

        public async Task<OrchestrationState> WaitForOrchestrationAsync(
            uint partitionId,
            string instanceId,
            string executionId,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
            {
                throw new ArgumentException(nameof(instanceId));
            }

            var request = new WaitRequestReceived()
            {
                PartitionId = partitionId,
                ClientId = this.ClientId,
                RequestId = Interlocked.Increment(ref this.SequenceNumber),
                InstanceId = instanceId,
                ExecutionId = executionId,
                Timeout = timeout,         
            };

            var response = await PerformRequestWithTimeoutAndCancellation(cancellationToken, request, false);            
            return ((WaitResponseReceived)response)?.OrchestrationState;
        }

        public async Task<OrchestrationState> GetOrchestrationStateAsync(uint partitionId, string instanceId)
        {
            if (string.IsNullOrWhiteSpace(instanceId))
            {
                throw new ArgumentException(nameof(instanceId));
            }

            var request = new StateRequestReceived()
            {
                PartitionId = partitionId,
                ClientId = this.ClientId,
                RequestId = Interlocked.Increment(ref this.SequenceNumber),
                InstanceId = instanceId,
                Timeout = DefaultTimeout,
            };

            var response = await PerformRequestWithTimeoutAndCancellation(CancellationToken.None, request, false);
            return ((StateResponseReceived)response)?.OrchestrationState;
        }

        public Task ForceTerminateTaskOrchestrationAsync(uint partitionId, string instanceId, string message)
        {
            var taskMessages = new[] { new TaskMessage
            {
                OrchestrationInstance = new OrchestrationInstance { InstanceId = instanceId },
                Event = new ExecutionTerminatedEvent(-1, message)
            } };

            var request = new ClientTaskMessagesReceived()
            {
                PartitionId = partitionId,
                ClientId = this.ClientId,
                RequestId = Interlocked.Increment(ref this.SequenceNumber),
                TaskMessages = taskMessages,
                Timeout = DefaultTimeout,
            };

            return PerformRequestWithTimeoutAndCancellation(CancellationToken.None, request, true);
        }

        public Task<string> GetOrchestrationHistoryAsync(uint partitionId, string instanceId, string executionId)
        {
            throw new NotSupportedException(); //TODO
        }

        public Task PurgeOrchestrationHistoryAsync(uint partitionId, DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotSupportedException(); //TODO
        }

     }
}
