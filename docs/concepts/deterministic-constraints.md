# Deterministic Constraints

Orchestration code must be **deterministic**—it must produce the same sequence of operations every time it runs with the same history. This is required because orchestrations are [replayed](replay-and-durability.md) to rebuild state after interruptions.

## The Golden Rule

> **The same input must always produce the same sequence of durable operations.**

Durable operations include:

- `ScheduleTask` / `ScheduleWithRetry`
- `CreateTimer`
- `WaitForExternalEvent`
- `CreateSubOrchestrationInstance`
- `ContinueAsNew`

## What NOT to Do

### ❌ Don't Use Current Time Directly

```csharp
// ❌ WRONG - Non-deterministic
if (DateTime.UtcNow > deadline)
{
    await context.ScheduleTask<string>(typeof(ExpiredActivity), input);
}

// ✅ CORRECT - Use orchestration time
if (context.CurrentUtcDateTime > deadline)
{
    await context.ScheduleTask<string>(typeof(ExpiredActivity), input);
}
```

### ❌ Don't Use Random Numbers

```csharp
// ❌ WRONG - Different on replay
var random = new Random();
if (random.Next(100) > 50)
{
    await context.ScheduleTask<string>(typeof(ActivityA), input);
}

// ✅ CORRECT - Get random value from activity
var randomValue = await context.ScheduleTask<int>(typeof(GetRandomNumberActivity), 100);
if (randomValue > 50)
{
    await context.ScheduleTask<string>(typeof(ActivityA), input);
}

// ✅ OR use a fixed seed
var random = new Random(42);  // Fixed seed
if (random.Next(100) > 50)
{
    await context.ScheduleTask<string>(typeof(ActivityA), input);
}
```

### ❌ Don't Use GUIDs Directly

```csharp
// ❌ WRONG - Different GUID on replay
var id = Guid.NewGuid().ToString();
await context.ScheduleTask<string>(typeof(ProcessActivity), id);

// ✅ CORRECT - Use orchestration's NewGuid
var id = context.NewGuid().ToString();
await context.ScheduleTask<string>(typeof(ProcessActivity), id);

// ✅ Also correct - Get from activity
var id = await context.ScheduleTask<string>(typeof(GenerateIdActivity), null);
```

### ❌ Don't Read Environment Variables

```csharp
// ❌ WRONG - May change between replays
var endpoint = Environment.GetEnvironmentVariable("API_ENDPOINT");
await context.ScheduleTask<string>(typeof(CallApiActivity), endpoint);

// ✅ CORRECT - Pass as input or read in activity
// Option 1: Pass as orchestration input
await context.ScheduleTask<string>(typeof(CallApiActivity), input.ApiEndpoint);

// Option 2: Read in activity
await context.ScheduleTask<string>(typeof(CallApiWithConfigActivity), input);
```

### ❌ Don't Make Network Calls

```csharp
// ❌ WRONG - Side effect, non-deterministic
var response = await httpClient.GetAsync("https://api.example.com/data");
var data = await response.Content.ReadAsStringAsync();

// ✅ CORRECT - Use activity for network calls
var data = await context.ScheduleTask<string>(typeof(FetchDataActivity), "https://api.example.com/data");
```

> [!NOTE]
> Awaiting a non-durable task like `httpClient.GetAsync` may cause the orchestration to hang indefinitely.

### ❌ Don't Access Databases

```csharp
// ❌ WRONG - Data may change between replays
var user = await dbContext.Users.FindAsync(userId);

// ✅ CORRECT - Use activity
var user = await context.ScheduleTask<User>(typeof(GetUserActivity), userId);
```

> [!NOTE]
> Awaiting a non-durable task like `dbContext.Users.FindAsync` may cause the orchestration to hang indefinitely.

### ❌ Don't Use Thread.Sleep

```csharp
// ❌ WRONG - Blocks thread, doesn't persist
Thread.Sleep(TimeSpan.FromMinutes(5));
await Task.Delay(TimeSpan.FromMinutes(5));

// ✅ CORRECT - Use durable timer
await context.CreateTimer(context.CurrentUtcDateTime.AddMinutes(5), true);
```

> [!NOTE]
> Awaiting a non-durable task like `Task.Delay` may cause the orchestration to hang indefinitely.

### ❌ Don't Use Mutable Static Variables

```csharp
// ❌ WRONG - State not preserved across replays
static int counter = 0;
counter++;
if (counter > 5) { ... }

// ✅ CORRECT - Use orchestration input/output for state
public override async Task<int> RunTask(OrchestrationContext context, int currentCount)
{
    currentCount++;
    if (currentCount > 5) { ... }
}
```

### ❌ Don't Use Non-Deterministic Collections

```csharp
// ❌ WRONG - HashSet and Dictionary iteration order is not guaranteed
var items = new HashSet<string> { "a", "b", "c" };
foreach (var item in items)
{
    await context.ScheduleTask<string>(typeof(ProcessActivity), item);
}

// ✅ CORRECT - Use ordered collection
var items = new List<string> { "a", "b", "c" };
foreach (var item in items)
{
    await context.ScheduleTask<string>(typeof(ProcessActivity), item);
}
```

### ❌ Don't Use Task.Run or Threading APIs

```csharp
// ❌ WRONG - Background tasks are non-deterministic and may not complete before replay
await Task.Run(() => ProcessData(input));

// ❌ WRONG - Manual thread creation is non-deterministic
var thread = new Thread(() => DoWork());
thread.Start();

// ❌ WRONG - ThreadPool work is non-deterministic
ThreadPool.QueueUserWorkItem(_ => ProcessItem(input));

// ✅ CORRECT - Use activities for background work
var result = await context.ScheduleTask<string>(typeof(ProcessDataActivity), input);

// ✅ CORRECT - Use fan-out pattern for parallel work
var tasks = input.Items.Select(item => 
    context.ScheduleTask<string>(typeof(ProcessItemActivity), item));
var results = await Task.WhenAll(tasks);
```

> [!NOTE]
> `Task.Run`, `ThreadPool.QueueUserWorkItem`, and manual thread creation introduce non-determinism because:
>
> - The work may complete at different times during replay
> - Background threads don't participate in orchestration checkpointing
> - Results are not captured in the orchestration history

## What IS Safe

### ✅ Local Computation

```csharp
// ✅ Safe - deterministic computation
var sum = input.Values.Sum();
var filtered = input.Items.Where(x => x.IsActive).ToList();
var formatted = $"Order {input.OrderId}: {input.Description}";
```

### ✅ Using Context Properties and Methods

```csharp
// ✅ Safe - consistent across replays
var instanceId = context.OrchestrationInstance.InstanceId;
var currentTime = context.CurrentUtcDateTime;
var newId = context.NewGuid();
```

### ✅ Conditional Logic Based on Durable Results

```csharp
// ✅ Safe - result comes from history during replay
var status = await context.ScheduleTask<OrderStatus>(typeof(GetStatusActivity), orderId);
if (status == OrderStatus.Approved)
{
    await context.ScheduleTask<string>(typeof(ProcessOrderActivity), orderId);
}
```

### ✅ Loops with Deterministic Bounds

```csharp
// ✅ Safe - loop bounds are deterministic
for (int i = 0; i < input.Items.Count; i++)
{
    await context.ScheduleTask<string>(typeof(ProcessItemActivity), input.Items[i]);
}
```

### ✅ Parallel Execution

```csharp
// ✅ Safe - Task.WhenAll is deterministic
var tasks = input.Items.Select(item => 
    context.ScheduleTask<string>(typeof(ProcessItemActivity), item));
var results = await Task.WhenAll(tasks);
```

## Summary Table

| Operation | Allowed in Orchestration? | Alternative |
| --------- | ------------------------- | ----------- |
| `DateTime.UtcNow` | ❌ No | `context.CurrentUtcDateTime` |
| `Guid.NewGuid()` | ❌ No | `context.NewGuid()` |
| `Random.Next()` | ❌ No | Get from activity |
| `Thread.Sleep()` / `Task.Delay()` | ❌ No | `context.CreateTimer()` |
| `Task.Run()` | ❌ No | Use activity or fan-out |
| `ThreadPool.QueueUserWorkItem()` | ❌ No | Use activity |
| Manual thread creation | ❌ No | Use activity |
| HTTP calls | ❌ No | Use activity |
| Database queries | ❌ No | Use activity |
| File I/O | ❌ No | Use activity |
| Environment variables | ⚠️ Avoid | Pass as input or read in activity |
| Static mutable state | ❌ No | Use orchestration state |
| `HashSet` or `Dictionary` iteration | ⚠️ Avoid | Use `List` or sorted collection |
| Local computation | ✅ Yes | — |
| String manipulation | ✅ Yes | — |
| LINQ queries (on local data) | ✅ Yes | — |

## Detecting Non-Determinism

### Runtime Detection

Some non-deterministic issues cause runtime errors:

```text
NonDeterministicOrchestrationException: The orchestration 'MyOrchestration' 
has a non-deterministic replay detected. The history expected 'TaskScheduled' 
for 'ActivityA' but got 'TaskScheduled' for 'ActivityB'.
```

### Static Analysis

Consider using analyzers or code reviews to catch issues:

- Review all `DateTime`, `Guid`, `Random` usage
- Search for HTTP client usage
- Check for `Thread.Sleep` or `Task.Delay`
- Check for `Task.Run`, `ThreadPool`, or `new Thread`

## Next Steps

- [Replay and Durability](replay-and-durability.md) — Why determinism matters
- [Versioning](../features/versioning.md) — Safely updating orchestration code
- [Error Handling](../features/error-handling.md) — Handling failures deterministically
