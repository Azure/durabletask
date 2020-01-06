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

namespace DurableTask.AzureStorage
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Messaging;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Tracking;
    using DurableTask.Core;

    class OrchestrationSessionManager : IDisposable
    {
        readonly Dictionary<string, OrchestrationSession> activeOrchestrationSessions = new Dictionary<string, OrchestrationSession>(StringComparer.OrdinalIgnoreCase);
        readonly ConcurrentDictionary<string, ControlQueue> ownedControlQueues = new ConcurrentDictionary<string, ControlQueue>();
        readonly LinkedList<PendingMessageBatch> pendingOrchestrationMessageBatches = new LinkedList<PendingMessageBatch>();
        readonly AsyncQueue<LinkedListNode<PendingMessageBatch>> readyForProcessingQueue = new AsyncQueue<LinkedListNode<PendingMessageBatch>>();
        readonly object messageAndSessionLock = new object();

        readonly string storageAccountName;
        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly AzureStorageOrchestrationServiceStats stats;
        readonly ITrackingStore trackingStore;
        readonly DispatchQueue fetchRuntimeStateQueue;

        public OrchestrationSessionManager(
            string storageAccountName,
            AzureStorageOrchestrationServiceSettings settings,
            AzureStorageOrchestrationServiceStats stats,
            ITrackingStore trackingStore)
        {
            this.storageAccountName = storageAccountName;
            this.settings = settings;
            this.stats = stats;
            this.trackingStore = trackingStore;

            this.fetchRuntimeStateQueue = new DispatchQueue(this.settings.MaxStorageOperationConcurrency);
        }

        internal IEnumerable<ControlQueue> Queues => this.ownedControlQueues.Values;

        public void AddQueue(string partitionId, ControlQueue controlQueue, CancellationToken cancellationToken)
        {
            if (this.ownedControlQueues.TryAdd(partitionId, controlQueue))
            {
                Task.Run(() => this.DequeueLoop(partitionId, controlQueue, cancellationToken));
            }
            else
            {
                AnalyticsEventSource.Log.PartitionManagerWarning(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    this.settings.WorkerId,
                    partitionId,
                    $"Attempted to add a control queue {controlQueue.Name} multiple times!",
                    Utils.ExtensionVersion);
            }
        }

        public void RemoveQueue(string partitionId)
        {
            if (this.ownedControlQueues.TryRemove(partitionId, out ControlQueue controlQueue))
            {
                controlQueue.Release();
            }
            else
            {
                AnalyticsEventSource.Log.PartitionManagerWarning(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    this.settings.WorkerId,
                    partitionId,
                    $"Attempted to remove control queue {controlQueue.Name}, which wasn't being watched!",
                    Utils.ExtensionVersion);
            }
        }

        async void DequeueLoop(string partitionId, ControlQueue controlQueue, CancellationToken cancellationToken)
        {
            AnalyticsEventSource.Log.PartitionManagerInfo(
                this.storageAccountName,
                this.settings.TaskHubName,
                this.settings.WorkerId,
                partitionId,
                $"Started listening for messages on queue {controlQueue.Name}.",
                Utils.ExtensionVersion);

            try
            {
                while (!controlQueue.IsReleased)
                {
                    // Every dequeue operation has a common trace ID so that batches of dequeued messages can be correlated together.
                    // Both the dequeue traces and the processing traces will share the same "related" trace activity ID.
                    Guid traceActivityId = AzureStorageOrchestrationService.StartNewLogicalTraceScope();

                    // This will block until either new messages arrive or the queue is released.
                    IReadOnlyList<MessageData> messages = await controlQueue.GetMessagesAsync(cancellationToken);
                    
                    if (messages.Count > 0)
                    {
                        this.AddMessageToPendingOrchestration(controlQueue, messages, traceActivityId, cancellationToken);
                    }
                }
            }
            finally
            {
                AnalyticsEventSource.Log.PartitionManagerInfo(
                    this.storageAccountName,
                    this.settings.TaskHubName,
                    this.settings.WorkerId,
                    partitionId,
                    $"Stopped listening for messages on queue {controlQueue.Name}.",
                    Utils.ExtensionVersion);
            }
        }

        internal void AddMessageToPendingOrchestration(
            ControlQueue controlQueue,
            IEnumerable<MessageData> queueMessages,
            Guid traceActivityId,
            CancellationToken cancellationToken)
        {
            // Conditions to consider:
            //  1. Do we need to create a new orchestration session or does one already exist?
            //  2. Do we already have a copy of this message?
            //  3. Do we need to add messages to a currently executing orchestration?
            lock (this.messageAndSessionLock)
            {
                LinkedListNode<PendingMessageBatch> node;

                var existingSessionMessages = new Dictionary<OrchestrationSession, List<MessageData>>();

                foreach (MessageData data in queueMessages)
                {
                    string instanceId = data.TaskMessage.OrchestrationInstance.InstanceId;
                    string executionId = data.TaskMessage.OrchestrationInstance.ExecutionId;

                    if (this.activeOrchestrationSessions.TryGetValue(instanceId, out OrchestrationSession session))
                    {
                        // If the target orchestration is already running we add the message to the session
                        // directly rather than adding it to the linked list. A null executionId value
                        // means that this is a management operation, like RaiseEvent or Terminate, which
                        // should be delivered to the current session.
                        if (executionId == null || session.Instance.ExecutionId == executionId)
                        {
                            List<MessageData> pendingMessages;
                            if (!existingSessionMessages.TryGetValue(session, out pendingMessages))
                            {
                                pendingMessages = new List<MessageData>();
                                existingSessionMessages.Add(session, pendingMessages);
                            }

                            pendingMessages.Add(data);
                        }
                        else if (data.TaskMessage.Event.Timestamp < session.RuntimeState.CreatedTime)
                        {
                            // This message was created for a previous generation of this instance.
                            // This is common for canceled timer fired events in ContinueAsNew scenarios.
                            session.DiscardMessage(data);
                        }
                        else
                        {
                            // Most likely this message was created for a new generation of the current
                            // instance. This can happen if a ContinueAsNew message arrives before the current
                            // session finished unloading. Defer the message so that it can be processed later.
                            session.DeferMessage(data);
                        }

                        continue;
                    }

                    // Walk backwards through the list of batches until we find one with a matching Instance ID.
                    // This is assumed to be more efficient than walking forward if most messages arrive in the queue in groups.
                    PendingMessageBatch targetBatch = null;
                    node = this.pendingOrchestrationMessageBatches.Last;
                    while (node != null)
                    {
                        PendingMessageBatch batch = node.Value;

                        if (batch.OrchestrationInstanceId == instanceId)
                        {
                            if (executionId == null || batch.OrchestrationExecutionId == executionId)
                            {
                                targetBatch = batch;
                                break;
                            }
                            else if (batch.OrchestrationExecutionId == null)
                            {
                                targetBatch = batch;
                                batch.OrchestrationExecutionId = executionId;
                                break;
                            }
                        }

                        node = node.Previous;
                    }

                    if (targetBatch == null)
                    {
                        targetBatch = new PendingMessageBatch(controlQueue, instanceId, executionId);
                        node = this.pendingOrchestrationMessageBatches.AddLast(targetBatch);

                        // Before the batch of messages can be processed, we need to download the latest execution state.
                        // This is done beforehand in the background as a performance optimization.
                        this.ScheduleOrchestrationStatePrefetch(node, traceActivityId, cancellationToken);
                    }

                    // New messages are added; duplicate messages are replaced
                    targetBatch.Messages.AddOrReplace(data);
                }

                // The session might be waiting for more messages. If it is, signal them.
                foreach (var pair in existingSessionMessages)
                {
                    OrchestrationSession session = pair.Key;
                    List<MessageData> newMessages = pair.Value;

                    // New messages are added; duplicate messages are replaced
                    session.AddOrReplaceMessages(newMessages);
                }
            }
        }

        // This method runs on a background task thread
        void ScheduleOrchestrationStatePrefetch(
            LinkedListNode<PendingMessageBatch> node,
            Guid traceActivityId,
            CancellationToken cancellationToken)
        {
            PendingMessageBatch batch = node.Value;

            // Do the fetch in a background thread
            this.fetchRuntimeStateQueue.EnqueueAndDispatch(async delegate
            {
                AnalyticsEventSource.SetLogicalTraceActivityId(traceActivityId);

                try
                {
                    if (batch.OrchestrationState == null)
                    {
                        OrchestrationHistory history = await this.trackingStore.GetHistoryEventsAsync(
                           batch.OrchestrationInstanceId,
                           batch.OrchestrationExecutionId,
                           cancellationToken);

                        batch.OrchestrationState = new OrchestrationRuntimeState(history.Events);
                        batch.ETag = history.ETag;
                        batch.LastCheckpointTime = history.LastCheckpointTime;
                    }

                    this.readyForProcessingQueue.Enqueue(node);
                }
                catch (OperationCanceledException)
                {
                    // shutting down
                }
                catch (Exception e)
                {
                    AnalyticsEventSource.Log.OrchestrationProcessingFailure(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        batch.OrchestrationInstanceId,
                        batch.OrchestrationExecutionId,
                        e.ToString(),
                        Utils.ExtensionVersion);

                    // Sleep briefly to avoid a tight failure loop.
                    await Task.Delay(TimeSpan.FromSeconds(5));

                    // This is a background operation so failure is not an option. All exceptions must be handled.
                    // To avoid starvation, we need to re-enqueue this async operation instead of retrying in a loop.
                    this.ScheduleOrchestrationStatePrefetch(node, traceActivityId, cancellationToken);
                }
            });
        }

        public async Task<OrchestrationSession> GetNextSessionAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // This call will block until:
                //  1) a batch of messages has been received for a particular instance and
                //  2) the history for that instance has been fetched
                LinkedListNode<PendingMessageBatch> node = await this.readyForProcessingQueue.DequeueAsync(cancellationToken);

                lock (this.messageAndSessionLock)
                {
                    PendingMessageBatch nextBatch = node.Value;
                    this.pendingOrchestrationMessageBatches.Remove(node);

                    if (!this.activeOrchestrationSessions.TryGetValue(nextBatch.OrchestrationInstanceId, out var existingSession))
                    {
                        OrchestrationInstance instance = nextBatch.OrchestrationState.OrchestrationInstance ??
                            new OrchestrationInstance
                            {
                                InstanceId = nextBatch.OrchestrationInstanceId,
                                ExecutionId = nextBatch.OrchestrationExecutionId,
                            };

                        Guid traceActivityId = AzureStorageOrchestrationService.StartNewLogicalTraceScope();

                        OrchestrationSession session = new OrchestrationSession(
                            this.storageAccountName,
                            this.settings.TaskHubName,
                            instance,
                            nextBatch.ControlQueue,
                            nextBatch.Messages,
                            nextBatch.OrchestrationState,
                            nextBatch.ETag,
                            nextBatch.LastCheckpointTime,
                            this.settings.ExtendedSessionIdleTimeout,
                            traceActivityId);

                        this.activeOrchestrationSessions.Add(instance.InstanceId, session);

                        return session;
                    }
                    else if (nextBatch.OrchestrationExecutionId == existingSession.Instance.ExecutionId)
                    {
                        // there is already an active session with the same execution id.
                        // The session might be waiting for more messages. If it is, signal them.
                        existingSession.AddOrReplaceMessages(node.Value.Messages);
                    }
                    else
                    {
                        // A message arrived for a different generation of an existing orchestration instance.
                        // Put it back into the ready queue so that it can be processed once the current generation
                        // is done executing.
                        if (this.readyForProcessingQueue.Count == 0)
                        {
                            // To avoid a tight dequeue loop, delay for a bit before putting this node back into the queue.
                            // This is only necessary when the queue is empty. The main dequeue thread must not be blocked
                            // by this delay, which is why we use Task.Delay(...).ContinueWith(...) instead of await.
                            Task.Delay(millisecondsDelay: 200).ContinueWith(_ =>
                            {
                                lock (this.messageAndSessionLock)
                                {
                                    this.pendingOrchestrationMessageBatches.AddLast(node);
                                    this.readyForProcessingQueue.Enqueue(node);
                                }
                            });
                        }
                        else
                        {
                            this.pendingOrchestrationMessageBatches.AddLast(node);
                            this.readyForProcessingQueue.Enqueue(node);
                        }
                    }
                }
            }

            return null;
        }

        public bool TryGetExistingSession(string instanceId, out OrchestrationSession session)
        {
            lock (this.messageAndSessionLock)
            {
                return this.activeOrchestrationSessions.TryGetValue(instanceId, out session);
            }
        }

        public bool TryReleaseSession(string instanceId, CancellationToken cancellationToken, out OrchestrationSession session)
        {
            // Taking this lock ensures we don't add new messages to a session we're about to release.
            lock (this.messageAndSessionLock)
            {
                // Release is local/in-memory only because instances are affinitized to queues and this
                // node already holds the lease for the target control queue.
                if (this.activeOrchestrationSessions.TryGetValue(instanceId, out session) &&
                    this.activeOrchestrationSessions.Remove(instanceId))
                {
                    // Put any unprocessed messages back into the pending buffer.
                    this.AddMessageToPendingOrchestration(
                        session.ControlQueue,
                        session.PendingMessages.Concat(session.DeferredMessages),
                        session.TraceActivityId,
                        cancellationToken);
                    return true;
                }
                else
                {
                    AnalyticsEventSource.Log.AssertFailure(
                        this.storageAccountName,
                        this.settings.TaskHubName,
                        $"{nameof(TryReleaseSession)}: Session for instance {instanceId} was not found!",
                        Utils.ExtensionVersion);
                    return false;
                }
            }
        }

        public void GetStats(
            out int pendingOrchestratorInstances,
            out int pendingOrchestrationMessages,
            out int activeOrchestrationSessions)
        {
            lock (this.messageAndSessionLock)
            {
                pendingOrchestratorInstances = this.pendingOrchestrationMessageBatches.Count;
                pendingOrchestrationMessages = (int)this.stats.PendingOrchestratorMessages.Value;
                activeOrchestrationSessions = this.activeOrchestrationSessions.Count;
            }
        }

        public virtual void Dispose()
        {
            this.fetchRuntimeStateQueue.Dispose();
            this.readyForProcessingQueue.Dispose();
        }

        class PendingMessageBatch
        {
            public PendingMessageBatch(ControlQueue controlQueue, string instanceId, string executionId)
            {
                this.ControlQueue = controlQueue ?? throw new ArgumentNullException(nameof(controlQueue));
                this.OrchestrationInstanceId = instanceId ?? throw new ArgumentNullException(nameof(instanceId));
                this.OrchestrationExecutionId = executionId; // null is expected in some cases
            }

            public ControlQueue ControlQueue { get; }
            public string OrchestrationInstanceId { get; }
            public string OrchestrationExecutionId { get; set; }
            public MessageCollection Messages { get; } = new MessageCollection();
            public OrchestrationRuntimeState OrchestrationState { get; set; }
            public string ETag { get; set; }
            public DateTime LastCheckpointTime { get; set; }
        }
    }
}
