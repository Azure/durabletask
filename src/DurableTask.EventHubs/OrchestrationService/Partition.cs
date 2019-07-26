using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DurableTask.Core;
using DurableTask.Core.History;

namespace DurableTask.EventHubs
{
    internal class Partition : Backend.IPartition
    {
        public uint PartitionId { get; private set; }
        public EventHubsOrchestrationServiceSettings Settings { get; private set; }

        public Storage.IPartitionState State { get; private set; }
        public Backend.ISender<Event> BatchSender { get; private set; }
        public WorkQueue<TaskActivityWorkItem> ActivityWorkItemQueue { get; private set; }
        public WorkQueue<TaskOrchestrationWorkItem> OrchestrationWorkItemQueue { get; private set; }

        public CancellationToken PartitionShutdownToken => this.partitionShutdown.Token;

        public BatchTimer<Event> PendingTimers { get; private set; }
        public PubSub<string, OrchestrationState> InstanceStatePubSub { get; private set; }
        public ConcurrentDictionary<long, ResponseWaiter> PendingResponses { get; private set; }

        private readonly CancellationTokenSource partitionShutdown;

        private static readonly Task completedTask = Task.FromResult<object>(null);

        public Partition(
            uint partitionId,
            Backend.ISender<Event> batchSender,
            EventHubsOrchestrationServiceSettings settings,
            WorkQueue<TaskActivityWorkItem> activityWorkItemQueue,
            WorkQueue<TaskOrchestrationWorkItem> orchestrationWorkItemQueue,
            CancellationToken cancellationToken)
        {
            this.PartitionId = partitionId;
            this.State = new EmulatedStorage();
            this.BatchSender = batchSender;
            this.Settings = settings;
            this.ActivityWorkItemQueue = activityWorkItemQueue;
            this.OrchestrationWorkItemQueue = orchestrationWorkItemQueue;

            this.partitionShutdown = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        }

        public Task<long> StartAsync()
        {
            // initialize collections for pending work
            this.PendingTimers = new BatchTimer<Event>(this.PartitionShutdownToken, this.TimersFired);
            this.InstanceStatePubSub = new PubSub<string, OrchestrationState>();
            this.PendingResponses = new ConcurrentDictionary<long, ResponseWaiter>();

            // restore from last snapshot
            return State.RestoreAsync(this);
        }
 
        public Task ProcessAsync(IEnumerable<PartitionEvent> batch)
        {
            foreach (var partitionEvent in batch)
            {
                System.Diagnostics.Trace.TraceInformation($"Part{this.PartitionId:D2}.{partitionEvent.QueuePosition:D7} Processing {partitionEvent}");

                var target = partitionEvent.GetTarget(this.State);
                this.State.Apply(target, partitionEvent);
            }
            return completedTask;
        }

        public async Task StopAsync()
        {
            this.partitionShutdown.Cancel();
            await this.State.ShutdownAsync();
        }

        public Task TakeCheckpoint(long position)
        {
            //TODO
            throw new NotImplementedException();
        }

        public void ConfirmDurablySent(IEnumerable<Event> sent)
        {
            long lastSendAck = -1;

            foreach (var evt in sent)
            {
                if (evt is TaskMessageReceived y && y.QueuePosition > lastSendAck)
                {
                    lastSendAck = y.QueuePosition;
                }
            }

            if (lastSendAck > -1)
            {
                this.BatchSender.Submit(new SentMessagesAcked()
                {
                    PartitionId = this.PartitionId,
                    LastAckedQueuePosition = lastSendAck,
                });
            }
        }

        private void TimersFired(List<Event> timersFired)
        {
            this.BatchSender.Submit(timersFired);
        }

        public class ResponseWaiter : CancellableCompletionSource<ClientEvent>
        {
            protected readonly ClientRequestEvent Request;
            protected readonly Partition Partition;

            public ResponseWaiter(CancellationToken token, ClientRequestEvent request, Partition partition) : base(token)
            {
                this.Request = request;
                this.Partition = partition;
                this.Partition.PendingResponses.TryAdd(Request.QueuePosition, this);
            }
            protected override void Cleanup()
            {
                this.Partition.PendingResponses.TryRemove(Request.QueuePosition, out var _);
                base.Cleanup();
            }
        }        

        public void TrySendResponse(ClientRequestEvent request, ClientEvent response)
        {
            if (this.PendingResponses.TryGetValue(request.QueuePosition, out var waiter))
            {
                waiter.TryFulfill(response);
            }
        }

        public void ObserveException(Exception error)
        {
            // TODO
            System.Diagnostics.Trace.TraceError($"exception observed: {error}");
        }

        /******************************/
        // Client requests
        /******************************/

        public async Task HandleAsync(WaitRequestReceived request)
        {
            try
            {
                var waiter = new OrchestrationWaiter(request, this);

                // start an async read from state
                var readTask = waiter.ReadFromStateAsync(this.State);

                var response = await waiter.Task;

                if (response != null)
                {
                    this.BatchSender.Submit(response);
                }
            }
            catch (TaskCanceledException)
            {
            }
            catch (Exception e)
            {
                this.ObserveException(e);
            }
        }

        private class OrchestrationWaiter : ResponseWaiter, PubSub<string, OrchestrationState>.IListener
        {
            public OrchestrationWaiter(WaitRequestReceived request, Partition partition) 
                : base(partition.PartitionShutdownToken, request, partition)
            {
                Key = request.InstanceId;
                partition.InstanceStatePubSub.Subscribe(this);
            }

            public string Key { get; private set; }

            public void Notify(OrchestrationState value)
            {
                if (value != null &&
                    value.OrchestrationStatus != OrchestrationStatus.Running &&
                    value.OrchestrationStatus != OrchestrationStatus.Pending &&
                    value.OrchestrationStatus != OrchestrationStatus.ContinuedAsNew)
                {
                    this.TryFulfill(new WaitResponseReceived()
                    {
                        ClientId = Request.ClientId,
                        RequestId = Request.RequestId,
                        OrchestrationState = value
                    });
                }
            }

            public async Task ReadFromStateAsync(Storage.IPartitionState state)
            {
                var orchestrationState = await state.ReadAsync(state.GetInstance(Key).GetOrchestrationState);
                this.Notify(orchestrationState);
            }

            protected override void Cleanup()
            {
                this.Partition.InstanceStatePubSub.Unsubscribe(this);
                base.Cleanup();
            }
        }

        public async Task HandleAsync(StateRequestReceived request)
        {
            try
            {
                var orchestrationState = await this.State.ReadAsync(this.State.GetInstance(request.InstanceId).GetOrchestrationState);

                var response = new StateResponseReceived()
                {
                    ClientId = request.ClientId,
                    RequestId = request.RequestId,
                    OrchestrationState = orchestrationState,
                };

                this.BatchSender.Submit(response);
            }
            catch (TaskCanceledException)
            {
            }
            catch (Exception e)
            {
                this.ObserveException(e);
            }
        }
    }
}
