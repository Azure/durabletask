# DurableTask.AzureStorage

## Orchestration message processing

### Prefetching

A single task hub worker listens on zero or more control queues. Each control queue processes messages for potentially many different orchestration instances. As a performance optimization, these messages are prefetched and buffered in memory using a background thread. As many as `ControlQueueBufferThreshold` messages can be buffered in memory at a time on a single task hub worker (64 by default).

Each message has an orchestration *instance ID* and *execution ID*. Prefetched messages are grouped by their instance and execution IDs into *batches*. For each batch, we also prefetch the orchestration runtime state from table storage. Once the runtime state has been fetched, the message batch is made available for processing. When the DTFx dispatcher requests the next work item, a batch of messages with their runtime state are converted into an orchestration *session*.

Because orchestration messages are prefetched on a background thread, messages may arrive for an orchestration instance that has already started processing an earlier batch. When this happens, these messages are added to the existing orchestration session. If *extended sessions* are enabled, these messages will be eventually picked up by the orchestration when its ready for its next batch. Otherwise, these messages will be added back to the message buffer after the existing orchestration session is released.

## Orchestration checkpoints

Checkpoints involve adding messages to Azure Storage queues and writing records to Azure Table Storage. Queues and tables cannot be kept transactionally consistent, so great care is taken to ensure we can recover from failures without losing data. To ensure eventual consistency, data operations are done in the following order:

1. All new messages are written to storage queues.
2. All new history rows are written to tables.
3. The batch of messages the current orchestration episode are deleted.

If there are failures between any of those steps, the episode can be replayed again. Currently there is no duplicate detection, so checkpoint failures can result in duplicate executions of activities and sub-orchestrations.

### Race conditions

The checkpoint order mentioned previously adds queue messages before writing history to Table storage. This behavior creates the possibility of interesting race conditions. For example, if an orchestration schedules an activity to run, its possible that the activity runs and returns a result before the orchestration has finished updating its history. This race could potentially cause a prefetch of the old orchestration history, which becomes invalid as soon as the still-running orchestration finishes writing the new history to Table storage!

The message/history race condition is handled in two different ways:

* If a message arrives for an *active* orchestration *on the same task hub worker*, instead of prefetching the history, we add that message to the active orchestration session. The session will then either process the message momentarily (when extended sessions are enabled) or will add it back to the prefetch buffer after it finishes writing the history. This is the common case since orchestration messages are usually processed by the same task hub worker.

* If a lease was reassigned to a different task hub worker after an orchestration started running, it's possible that the orchestration instance will be active on one worker but messages will arrive on another. In this case, we cannot know whether the orchestration associated with a message is active or not. Same story if the worker is recycled in the middle of orchestration process. Instead of relying on an in-memory list of active orchestration sessions, we have to use timestamps to detect race conditions. Each message has a timestamp and the orchestration has a "last checkpoint completed" timestamp. If the timestamp of a message comes after the timestamp of the checkpoint, then we know the orchestration is not yet ready to process the message. We handle this by putting the message back on the queue and tracing a warning message (`AbandoningMessage`). The orchestration's "last checkpoint completed" timestamp will be updated soon, so eventually the abandoned message can be processed successfully.

We considered removing the possibility of these race conditions by writing to the history table first before adding messages to the Storage queues. However, this would require us to automatically detect incomplete checkpoints and could potentially be more expensive in terms of I/O. The current approach of handling race conditions is thought to be much more efficient in comparison, which is important given the scalability limitations of Azure Storage.
