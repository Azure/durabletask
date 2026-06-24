# Eternal Orchestrations

Eternal orchestrations are long-running workflows that run indefinitely by periodically restarting themselves. This pattern is useful for monitoring, scheduling, and other recurring tasks.

## The ContinueAsNew Pattern

### Basic Eternal Orchestration

```csharp
public class MonitorOrchestration : TaskOrchestration<object, MonitorInput>
{
    public override async Task<object> RunTask(
        OrchestrationContext context, 
        MonitorInput input)
    {
        // Do the monitoring work
        await context.ScheduleTask<bool>(typeof(CheckHealthActivity), input.Target);
        
        // Wait for next interval
        await context.CreateTimer(
            context.CurrentUtcDateTime.AddMinutes(input.IntervalMinutes),
            true);
        
        // Restart with fresh history
        context.ContinueAsNew(input);
        
        return null;  // Has no effect since ContinueAsNew was called
    }
}
```

### Why ContinueAsNew?

Without `ContinueAsNew`, orchestration history grows unbounded:

```text
// After 1000 iterations without ContinueAsNew:
History size: 10,000+ events
Memory usage: High
Replay time: Slow
```

An orchestration with an unbounded history can lead to severe performance degradation and process crashes due to OutOfMemoryExceptions.

With `ContinueAsNew`:

```text
// After 1000 iterations with ContinueAsNew:
History size: ~10 events (reset each iteration)
Memory usage: Low
Replay time: Fast
```

## ContinueAsNew Behavior

### What Happens

1. `ContinueAsNew(newInput)` is called
2. Current execution completes when `RunTask` returns
3. New execution starts with:
   - Same instance ID
   - Fresh (empty) history
   - New input provided to `ContinueAsNew`

### Status Transitions

```text
Running → ContinuedAsNew → Running (new execution)
```

### History Reset

Old history is usually **replaced**, not appended. The previous execution's history can optionally be retained for auditing (provider-dependent).

## Common Patterns

### Periodic Monitoring

```csharp
public override async Task<object> RunTask(
    OrchestrationContext context, 
    MonitorConfig config)
{
    // Check system health
    var health = await context.ScheduleTask<HealthStatus>(
        typeof(CheckHealthActivity),
        config.Endpoint);
    
    // Alert if unhealthy
    if (!health.IsHealthy)
    {
        await context.ScheduleTask<bool>(
            typeof(SendAlertActivity),
            new Alert { Endpoint = config.Endpoint, Status = health });
    }
    
    // Wait before next check
    await context.CreateTimer(
        context.CurrentUtcDateTime.AddMinutes(config.CheckIntervalMinutes),
        true);
    
    // Continue forever
    context.ContinueAsNew(config);
    return null;
}
```

### Job Queue Processor

```csharp
public override async Task<object> RunTask(
    OrchestrationContext context, 
    QueueConfig config)
{
    // Get next batch of jobs
    var jobs = await context.ScheduleTask<List<Job>>(
        typeof(GetPendingJobsActivity),
        new GetJobsInput { MaxCount = config.BatchSize });
    
    if (jobs.Any())
    {
        // Process jobs in parallel
        var tasks = jobs.Select(job =>
            context.ScheduleTask<JobResult>(typeof(ProcessJobActivity), job));
        await Task.WhenAll(tasks);
    }
    
    // Short delay if no jobs, to avoid busy-waiting
    var delay = jobs.Any() 
        ? TimeSpan.FromSeconds(1) 
        : TimeSpan.FromSeconds(30);
    
    await context.CreateTimer(context.CurrentUtcDateTime.Add(delay), true);
    
    context.ContinueAsNew(config);
    return null;
}
```

### Cron Scheduler

```csharp
public override async Task<object> RunTask(
    OrchestrationContext context, 
    CronSchedule schedule)
{
    // Calculate next run time
    var nextRun = GetNextCronTime(schedule.CronExpression, context.CurrentUtcDateTime);
    
    // Wait until scheduled time
    await context.CreateTimer(nextRun, true);
    
    // Execute the scheduled task
    await context.ScheduleTask<bool>(typeof(ScheduledTaskActivity), schedule.TaskInput);
    
    // Continue to next scheduled run
    context.ContinueAsNew(schedule);
    return null;
}
```

### Stateful Aggregator

```csharp
// The 4-type-parameter base declares the event payload type (DataPoint), so the framework
// deserializes incoming events for us and hands OnEvent a typed value.
public class AggregatorOrchestration : TaskOrchestration<object, AggregatorState, DataPoint, string>
{
    // Completed by OnEvent when a "NewData" event arrives
    TaskCompletionSource<DataPoint> dataHandle;

    public override async Task<object> RunTask(
        OrchestrationContext context, 
        AggregatorState state)
    {
        // Initialize state on first run
        state ??= new AggregatorState { Count = 0, Total = 0 };
        
        // Wait for data event or periodic save
        this.dataHandle = new TaskCompletionSource<DataPoint>();
        var eventTask = this.dataHandle.Task;

        using var cts = new CancellationTokenSource();
        var saveTask = context.CreateTimer(
            context.CurrentUtcDateTime.AddMinutes(5),
            true,
            cts.Token);
        
        var winner = await Task.WhenAny(eventTask, saveTask);
        cts.Cancel();
        
        if (winner == eventTask)
        {
            // Update aggregations
            var data = await eventTask;
            state.Count++;
            state.Total += data.Value;
            state.LastUpdated = context.CurrentUtcDateTime;
        }
        else
        {
            // Periodic save
            if (state.Count > 0)
            {
                await context.ScheduleTask<bool>(
                    typeof(SaveAggregationActivity),
                    state);
            }
        }
        
        // Check for termination signal
        if (state.ShouldTerminate)
        {
            return state;  // Actually return and complete
        }
        
        // Continue with updated state
        context.ContinueAsNew(state);
        return null;
    }

    // TrySetResult keeps a duplicate or late "NewData" event from faulting the orchestration.
    public override void OnEvent(OrchestrationContext context, string name, DataPoint input)
    {
        if (name == "NewData")
        {
            this.dataHandle?.TrySetResult(input);
        }
    }
}
```

### With Maximum Iterations

```csharp
public override async Task<ProcessingResult> RunTask(
    OrchestrationContext context, 
    ProcessingState state)
{
    state.Iteration++;
    
    // Do work
    var result = await context.ScheduleTask<bool>(
        typeof(ProcessBatchActivity),
        state.CurrentBatch);
    
    // Check completion conditions
    if (state.Iteration >= state.MaxIterations)
    {
        return new ProcessingResult 
        { 
            Completed = true, 
            Iterations = state.Iteration 
        };
    }
    
    if (!state.HasMoreWork)
    {
        return new ProcessingResult 
        { 
            Completed = true, 
            Iterations = state.Iteration 
        };
    }
    
    // Wait and continue
    await context.CreateTimer(
        context.CurrentUtcDateTime.AddSeconds(state.DelaySeconds),
        true);
    
    context.ContinueAsNew(state);
    return null;  // No effect due to ContinueAsNew
}
```

## FIFO Job Queues

You can use orchestrations to implement FIFO job queues. Use one orchestration instance per logical "queue" to serialize jobs. Because an orchestration runs its own logic sequentially, only one job per queue is ever active and jobs run in FIFO order, while different queues run in parallel. Incoming jobs are buffered via the [External Events](external-events.md) pattern.  `ContinueAsNew` carries any unprocessed jobs forward so none are lost when history is reset.

```csharp
// One instance per resource ID applies each update to a primary store then a replica,
// strictly in FIFO order. Expected event: "Enqueue" (ResourceUpdate).
// The 4-type-parameter base declares ResourceUpdate as the event payload type, so the
// framework deserializes incoming events and passes OnEvent a typed value.
public class ResourceUpdateQueueOrchestration : TaskOrchestration<object, QueueState, ResourceUpdate, string>
{
    const int MaxUpdatesPerGeneration = 100;  // Max updates processed per generation

    // Rebuilt deterministically on replay, since OnEvent replays in original enqueue order.
    readonly Queue<ResourceUpdate> inbox = new Queue<ResourceUpdate>();
    TaskCompletionSource<bool> newWork;  // Wakes RunTask when an update arrives while parked.

    public override async Task<object> RunTask(OrchestrationContext context, QueueState state)
    {
        // Re-seed with updates carried over from the previous generation (oldest first, so
        // they stay ahead of any newly arriving events and FIFO order is preserved).
        foreach (ResourceUpdate update in state?.Backlog ?? Enumerable.Empty<ResourceUpdate>())
        {
            this.inbox.Enqueue(update);
        }

        // Block until the first update arrives.
        if (this.inbox.Count == 0)
        {
            this.newWork = new TaskCompletionSource<bool>();
            await this.newWork.Task;
            this.newWork = null;
        }

        // Drain in FIFO order. Limit the number processed per generation to bound history size; if there are more, we'll pick up the rest in the next generation.
        for (int processed = 0; this.inbox.Count > 0 && processed < MaxUpdatesPerGeneration; processed++)
        {
            ResourceUpdate next = this.inbox.Dequeue();
            await context.ScheduleTask<bool>(typeof(UpdatePrimaryStoreActivity), next);
            await context.ScheduleTask<bool>(typeof(UpdateReplicaStoreActivity), next);
        }

        // Reset history, carrying any updates that arrived while we were busy.
        context.ContinueAsNew(new QueueState { Backlog = this.inbox.ToArray() });
        return null;
    }

    public override void OnEvent(OrchestrationContext context, string name, ResourceUpdate input)
    {
        if (name == "Enqueue")
        {
            this.inbox.Enqueue(input);
            this.newWork?.TrySetResult(true);
        }
    }
}

public class QueueState
{
    // Updates carried over from the previous generation, oldest first.
    public ResourceUpdate[] Backlog { get; set; } = Array.Empty<ResourceUpdate>();
}
```

Producers enqueue by sending an `Enqueue` event to the per-resource instance. The first update atomically creates the queue and delivers the event, so there's no window where it could be lost:

```csharp
static string InstanceIdFor(string resourceId) => $"resource-update-queue:{resourceId}";

static readonly OrchestrationStatus[] ActiveQueueStatuses =
{
    OrchestrationStatus.Pending,
    OrchestrationStatus.Running,
    OrchestrationStatus.ContinuedAsNew,
};

public static async Task EnqueueAsync(TaskHubClient client, ResourceUpdate update)
{
    try
    {
        // First update: atomically create the queue and deliver the update.
        await client.CreateOrchestrationInstanceWithRaisedEventAsync(
            typeof(ResourceUpdateQueueOrchestration),
            InstanceIdFor(update.ResourceId),
            new QueueState(),
            dedupeStatuses: ActiveQueueStatuses,
            eventName: "Enqueue",
            eventData: update);
    }
    catch (OrchestrationAlreadyExistsException)
    {
        // Queue already active for this resource ID; just append the update.
        await client.RaiseEventAsync(
            new OrchestrationInstance { InstanceId = InstanceIdFor(update.ResourceId) },
            "Enqueue",
            update);
    }
}
```

> [!NOTE]
> On Azure Storage (the default `Carryover` behavior), events that arrive during the brief `ContinueAsNew` transition are automatically re-delivered to the next generation, so nothing is lost. The Service Fabric provider instead uses `Ignore` and drops them — there, treat the producer as the source of truth and re-drive any unacknowledged update (using `UpdateId` for idempotency).

## Graceful Termination

### Using External Events

```csharp
public class WorkerOrchestration : TaskOrchestration<object, Config, bool, string>
{
    // Completed by OnEvent when a "Stop" event arrives.
    TaskCompletionSource<bool> stopHandle;

    public override async Task<object> RunTask(
        OrchestrationContext context, 
        Config config)
    {
        this.stopHandle = new TaskCompletionSource<bool>();
        Task stopTask = this.stopHandle.Task;

        using var cts = new CancellationTokenSource();
        Task workTask = DoWorkAsync(context, config);
        Task timerTask = context.CreateTimer(
            context.CurrentUtcDateTime.AddMinutes(1),
            true,
            cts.Token);
        
        Task winner = await Task.WhenAny(stopTask, workTask, timerTask);
        cts.Cancel();
        
        if (winner == stopTask)
        {
            // Graceful shutdown
            return new Result { StoppedGracefully = true };
        }
        
        context.ContinueAsNew(config);
        return null;
    }

    // TrySetResult keeps a duplicate "Stop" event from faulting the orchestration.
    public override void OnEvent(OrchestrationContext context, string name, bool input)
    {
        if (name == "Stop")
        {
            this.stopHandle?.TrySetResult(input);
        }
    }
}
```

## Best Practices

### 1. Be Careful with Tight Loops

Immediate restarts via `ContinueAsNew` can be useful when processing batches of external events to minimize latency. However, be careful to avoid tight loops that do no meaningful work:

```csharp
// ✅ OK - immediate restart when processing a batch of work
if (pendingItems.Any())
{
    await ProcessBatchAsync(context, pendingItems);
    context.ContinueAsNew(state);  // Restart immediately to check for more
    return null;
}

// ✅ Good - add delay when no work to do
await context.CreateTimer(context.CurrentUtcDateTime.AddSeconds(30), true);
context.ContinueAsNew(state);

// ⚠️ Risky - tight loop with no work and no delay
var items = await context.ScheduleTask<List<Item>>(typeof(GetItemsActivity), null);
if (!items.Any())
{
    context.ContinueAsNew(state);  // Immediately restarts even with no work!
    return null;
}
```

### 2. Carry Forward Essential State

```csharp
// ✅ Good - preserves necessary context
context.ContinueAsNew(new State
{
    TotalProcessed = state.TotalProcessed + batchSize,
    LastCheckpoint = context.CurrentUtcDateTime,
    Config = state.Config
});

// ⚠️ Careful - losing important state
context.ContinueAsNew(state.Config);  // Lost TotalProcessed
```

### 3. Provide Termination Mechanism

```csharp
// ✅ Good - can be stopped gracefully
if (config.StopRequested || iterationCount > maxIterations)
{
    return finalResult;
}
context.ContinueAsNew(config);
```

### 4. Monitor History Size

If `ContinueAsNew` isn't called frequently enough, history can still grow. Consider continuing after a fixed number of operations:

```csharp
if (state.OperationsSinceRestart > 100)
{
    state.OperationsSinceRestart = 0;
    context.ContinueAsNew(state);
    return null;
}
```

## Next Steps

- [Timers](timers.md) — Creating durable delays
- [External Events](external-events.md) — Signaling eternal orchestrations
- [Replay and Durability](../concepts/replay-and-durability.md) — Understanding history growth
