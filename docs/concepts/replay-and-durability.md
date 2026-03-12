# Replay and Durability

The Durable Task Framework achieves durability through an **event-sourcing** pattern. Understanding how replay works is essential for writing correct orchestrations.

## How Durability Works

### The Problem

Traditional workflows have a problem: if the process crashes, in-progress state is lost.

```text
Process starts → Workflow runs → CRASH → State lost ❌
```

### The Solution: Event Sourcing

DTFx persists every decision as an event in the history:

```text
Orchestration executes → Event recorded → (Crash) → Replay from history → Continue ✅
```

## The Replay Model

### First Execution

When an orchestration runs for the first time:

```csharp
public override async Task<string> RunTask(OrchestrationContext context, string input)
{
    var a = await context.ScheduleTask<string>(typeof(ActivityA), input);  // Executes, records TaskScheduled
    var b = await context.ScheduleTask<string>(typeof(ActivityB), a);      // Executes, records TaskScheduled
    return b;
}
```

**History after first execution:**

```text
1. ExecutionStarted { Input: "hello" }
2. TaskScheduled { Name: "ActivityA" }
3. TaskCompleted { Result: "A-result" }
4. TaskScheduled { Name: "ActivityB" }
5. TaskCompleted { Result: "B-result" }
6. ExecutionCompleted { Result: "B-result" }
```

### Replay After Crash

If the process crashes and restarts, the orchestration **replays**:

1. Framework loads the history from storage
2. The orchestration's `RunTask` method executes again from the beginning
3. Each `await` checks if there's already a result in history
4. If result exists, return it immediately (no actual execution)
5. If no result, schedule the work and wait

```csharp
// During replay:
var a = await context.ScheduleTask<string>(typeof(ActivityA), input);  
// ↑ Sees TaskCompleted in history, returns "A-result" immediately

var b = await context.ScheduleTask<string>(typeof(ActivityB), a);      
// ↑ Sees TaskCompleted in history, returns "B-result" immediately
```

### Partial Replay

If an orchestration is waiting for an activity:

```text
1. ExecutionStarted { Input: "hello" }
2. TaskScheduled { Name: "ActivityA" }
3. TaskCompleted { Result: "A-result" }
4. TaskScheduled { Name: "ActivityB" }
← Activity B is still running
```

When Activity B completes, the orchestration replays:

```csharp
var a = await context.ScheduleTask<string>(typeof(ActivityA), input);  
// ↑ Returns "A-result" from history

var b = await context.ScheduleTask<string>(typeof(ActivityB), a);      
// ↑ Finds new TaskCompleted event, returns result

return b; // Orchestration completes
```

## Checkpointing

### When Checkpoints Occur

The orchestration state is checkpointed (saved) when:

1. An `await` yields control back to the framework
2. The orchestration completes or fails
3. `ContinueAsNew` is called and the current execution ends

### What Gets Saved

- Complete event history
- Custom status (if set)
- Input and output (if any)

### What Doesn't Get Saved

- Local variables (they're rebuilt during replay)
- In-memory state outside the orchestration

## Understanding Context.IsReplaying

The `IsReplaying` property tells you if the orchestration is replaying:

```csharp
public override async Task<string> RunTask(OrchestrationContext context, string input)
{
    // This code runs during EVERY replay
    var greeting = $"Hello, {input}";
    
    if (!context.IsReplaying)
    {
        // This only runs during the FIRST execution of this code path
        _logger.LogInformation("Processing input: {Input}", input);
    }
    
    var result = await context.ScheduleTask<string>(typeof(MyActivity), greeting);
    
    return result;
}
```

### When to Use IsReplaying

| Use Case | Use IsReplaying? |
| -------- | ---------------- |
| Logging | ✅ Yes - avoid duplicate logs |
| Metrics | ✅ Yes - avoid double-counting |
| Business logic | ❌ No - should work identically during replay |
| Side effects | ❌ No - use activities instead |

## Why Determinism Matters

Because orchestrations replay, they **must** produce the same sequence of events every time:

### Example: Non-Deterministic Code (BAD)

```csharp
// ❌ WRONG - Different result on each replay
public override async Task<string> RunTask(OrchestrationContext context, string input)
{
    if (DateTime.UtcNow.Hour < 12)  // Different on replay!
    {
        return await context.ScheduleTask<string>(typeof(MorningActivity), input);
    }
    return await context.ScheduleTask<string>(typeof(EveningActivity), input);
}
```

If the orchestration starts at 11:55 AM and replays at 12:05 PM, it will try to match `EveningActivity` against a history containing `MorningActivity` → **crash**.

### Example: Deterministic Code (GOOD)

```csharp
// ✅ CORRECT - Same result on every replay
public override async Task<string> RunTask(OrchestrationContext context, string input)
{
    if (context.CurrentUtcDateTime.Hour < 12)  // Same value during replay!
    {
        return await context.ScheduleTask<string>(typeof(MorningActivity), input);
    }
    return await context.ScheduleTask<string>(typeof(EveningActivity), input);
}
```

## History Events

Common events in the orchestration history:

| Event | Description |
| ----- | ----------- |
| `ExecutionStarted` | Orchestration started |
| `TaskScheduled` | Activity was scheduled |
| `TaskCompleted` | Activity completed successfully |
| `TaskFailed` | Activity failed |
| `SubOrchestrationInstanceCreated` | Sub-orchestration started |
| `SubOrchestrationInstanceCompleted` | Sub-orchestration completed |
| `TimerCreated` | Timer was created |
| `TimerFired` | Timer elapsed |
| `EventRaised` | External event received |
| `ExecutionCompleted` | Orchestration completed |
| `ExecutionFailed` | Orchestration failed |
| `ExecutionTerminated` | Orchestration was terminated |
| `ContinueAsNew` | Orchestration restarted |

## Viewing History

### Via Client

```csharp
var history = await client.GetOrchestrationHistoryAsync(instance);
foreach (var evt in history)
{
    Console.WriteLine($"{evt.EventType}: {evt.Timestamp}");
}
```

### What History Tells You

- Exact sequence of operations
- Timing of each step
- Input/output of each activity
- Where failures occurred

## Performance Implications

### History Growth

Every operation adds to the history. Large histories can impact:

- **Memory** — Full history is loaded into memory during replay
- **Latency** — More events = longer replay time
- **Storage** — More data to persist and transfer (the exact impact depends on the storage provider)

### Mitigation Strategies

1. **Use `ContinueAsNew`** for long-running orchestrations:

   ```csharp
   if (context.CurrentUtcDateTime > startTime.AddHours(24))
   {
       context.ContinueAsNew(newState);  // Reset history
       return default;
   }
   ```

2. **Batch operations** in activities instead of many small activities

3. **Sub-orchestrations** for logical groupings (separate history)

4. **Purge completed instances** periodically

## Next Steps

- [Deterministic Constraints](deterministic-constraints.md) — Rules for writing deterministic code
- [Eternal Orchestrations](../features/eternal-orchestrations.md) — Managing long-running workflows
- [Versioning](../features/versioning.md) — Updating orchestration code safely
