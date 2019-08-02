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
    internal class Partition : Backend.IPartition
    {
        public uint PartitionId { get; private set; }
        public Func<string, uint> PartitionFunction { get; private set; }

        public EventHubsOrchestrationServiceSettings Settings { get; private set; }

        public Storage.IPartitionState State { get; private set; }
        public Backend.ISender BatchSender { get; private set; }
        public WorkQueue<TaskActivityWorkItem> ActivityWorkItemQueue { get; private set; }
        public WorkQueue<TaskOrchestrationWorkItem> OrchestrationWorkItemQueue { get; private set; }

        public CancellationToken PartitionShutdownToken => this.partitionShutdown.Token;

        public BatchTimer<Event> PendingTimers { get; private set; }
        public PubSub<string, OrchestrationState> InstanceStatePubSub { get; private set; }
        public ConcurrentDictionary<long, ResponseWaiter> PendingResponses { get; private set; }

        private readonly CancellationTokenSource partitionShutdown;

        private static readonly Task completedTask = Task.FromResult<object>(null);

        public AsyncLocal<string> TraceContext { get; private set; }

        public Partition(
            uint partitionId,
            Func<string,uint> partitionFunction,
            Backend.ISender batchSender,
            EventHubsOrchestrationServiceSettings settings,
            WorkQueue<TaskActivityWorkItem> activityWorkItemQueue,
            WorkQueue<TaskOrchestrationWorkItem> orchestrationWorkItemQueue,
            CancellationToken cancellationToken)
        {
            this.PartitionId = partitionId;
            this.PartitionFunction = partitionFunction;
            this.State = new EmulatedStorage();
            this.BatchSender = batchSender;
            this.Settings = settings;
            this.ActivityWorkItemQueue = activityWorkItemQueue;
            this.OrchestrationWorkItemQueue = orchestrationWorkItemQueue;

            this.partitionShutdown = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            this.TraceContext = new AsyncLocal<string>();
        }

        public async Task<long> StartAsync()
        {
            // initialize collections for pending work
            this.PendingTimers = new BatchTimer<Event>(this.PartitionShutdownToken, this.TimersFired);
            this.InstanceStatePubSub = new PubSub<string, OrchestrationState>();
            this.PendingResponses = new ConcurrentDictionary<long, ResponseWaiter>();

            // restore from last snapshot
            this.TraceContext.Value = "restore";
            var resumeFrom = await State.RestoreAsync(this);
            this.TraceContext.Value = null;

            return resumeFrom;
        }

        public Task ProcessAsync(IEnumerable<PartitionEvent> batch)
        {
            foreach (var partitionEvent in batch)
            {
                this.TraceContext.Value = $"{partitionEvent.QueuePosition:D7}   ";

                this.TraceReceive(partitionEvent);

                this.State.Process(partitionEvent);
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

        private void TimersFired(List<Event> timersFired)
        {
            this.TraceContext.Value = "TWorker";
            foreach (var t in timersFired)
            {
                this.Submit(t);
            }
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
                waiter.TrySetResult(response);
            }
        }

        public void Submit(Event evt, Backend.ISendConfirmationListener listener = null)
        {
            this.Trace($"Sending {evt}");
            this.BatchSender.Submit(evt, listener);
        }

        public void ReportError(string msg, Exception e)
        {
            System.Diagnostics.Trace.TraceError($"Part{this.PartitionId:D2} !!! {msg}: {e}");
        }

        [Conditional("DEBUG")]
        public void TraceReceive(PartitionEvent evt)
        {
            System.Diagnostics.Trace.TraceInformation($"Part{this.PartitionId:D2}.{evt.QueuePosition:D7} Processing {evt}");
        }

        [Conditional("DEBUG")]
        public void Trace(string msg)
        {
            var context = this.TraceContext.Value;
            if (string.IsNullOrEmpty(context))
            {
                System.Diagnostics.Trace.TraceInformation($"Part{this.PartitionId:D2}         {msg}");
            }
            else
            {
                System.Diagnostics.Trace.TraceInformation($"Part{this.PartitionId:D2}.{context} {msg}");
            }
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
                    this.Submit(response);
                }
            }
            catch (TaskCanceledException)
            {
            }
            catch (Exception e)
            {
                this.ReportError($"Exception while handling {request.GetType().Name}", e);
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
                    this.TrySetResult(new WaitResponseReceived()
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

                this.Submit(response);
            }
            catch (TaskCanceledException)
            {
            }
            catch (Exception e)
            {
                this.ReportError($"Exception while handling {request.GetType().Name}", e);
            }
        }
    }
}
