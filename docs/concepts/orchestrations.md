# Orchestrations

Orchestrations are the core building blocks of the Durable Task Framework. They define durable, long-running workflows that coordinate activities, sub-orchestrations, timers, and external events.

## Creating an Orchestration

### Basic Structure

Inherit from `TaskOrchestration<TResult, TInput>`:

```csharp
using DurableTask.Core;

public class OrderProcessingOrchestration : TaskOrchestration<OrderResult, OrderInput>
{
    public override async Task<OrderResult> RunTask(
        OrchestrationContext context, 
        OrderInput input)
    {
        // Orchestration logic here
        return new OrderResult { Success = true };
    }
}
```

### Type Parameters

- `TResult` — The return type of the orchestration
- `TInput` — The input type passed when starting the orchestration

### Registration

Register orchestrations with the worker:

```csharp
var worker = new TaskHubWorker(service, loggerFactory);
worker.AddTaskOrchestrations(typeof(OrderProcessingOrchestration));
await worker.StartAsync();
```

## OrchestrationContext

The `OrchestrationContext` provides APIs for scheduling durable operations:

### Scheduling Activities

```csharp
// Schedule an activity and wait for result
var result = await context.ScheduleTask<string>(typeof(MyActivity), input);

// Schedule with retry options
var options = new RetryOptions(
    firstRetryInterval: TimeSpan.FromSeconds(5),
    maxNumberOfAttempts: 3);
    
var result = await context.ScheduleWithRetry<string>(
    typeof(MyActivity), 
    options, 
    input);
```

### Creating Timers

Timers allow orchestrations to wait for a specific time or duration. They are durable and survive process restarts.

```csharp
// Wait for a specific time
await context.CreateTimer(context.CurrentUtcDateTime.AddHours(1), true);

// Use for delays (not Thread.Sleep!)
await context.CreateTimer(context.CurrentUtcDateTime.AddMinutes(5), true);
```

> [!IMPORTANT]
>
> - Never use `Thread.Sleep` for delays in orchestrations.
> - Always use `context.CurrentUtcDateTime` for time calculations to ensure determinism.
> - Timers are cancellable using `CancellationToken` and must be cancelled if no longer needed.

### Waiting for External Events

Orchestrations can pause and wait for external events sent from client code or other orchestrations.

```csharp
// Wait indefinitely for an event
var approvalData = await context.WaitForExternalEvent<ApprovalData>("ApprovalReceived");

// Wait with timeout
using var cts = new CancellationTokenSource();
var timerTask = context.CreateTimer(context.CurrentUtcDateTime.AddDays(1), true, cts.Token);
var eventTask = context.WaitForExternalEvent<ApprovalData>("ApprovalReceived");

var winner = await Task.WhenAny(timerTask, eventTask);
if (winner == eventTask)
{
    // Timer cancelled since event was received (this is important)
    cts.Cancel();
    var approval = await eventTask;
    // Process approval
}
else
{
    // Timeout - escalate or reject
}
```

### Sub-Orchestrations

```csharp
// Start a sub-orchestration
var subResult = await context.CreateSubOrchestrationInstance<SubResult>(
    typeof(SubOrchestration),
    subInput);

// With custom instance ID
var subResult = await context.CreateSubOrchestrationInstance<SubResult>(
    typeof(SubOrchestration),
    "sub-instance-123",
    subInput);
```

### Continue As New

```csharp
// Restart orchestration with new input (eternal orchestrations)
context.ContinueAsNew(newInput);
return default; // Return value is ignored
```

## Orchestration Patterns

### Sequential Execution

```csharp
public override async Task<string> RunTask(OrchestrationContext context, string input)
{
    var step1 = await context.ScheduleTask<string>(typeof(Step1Activity), input);
    var step2 = await context.ScheduleTask<string>(typeof(Step2Activity), step1);
    var step3 = await context.ScheduleTask<string>(typeof(Step3Activity), step2);
    return step3;
}
```

### Fan-Out/Fan-In (Parallel Execution)

The fan-out/fan-in pattern allows multiple tasks to be executed in parallel, with the orchestration waiting for all to complete before proceeding.

```csharp
public override async Task<int[]> RunTask(OrchestrationContext context, int[] inputs)
{
    // Fan-out: Start all tasks in parallel
    var tasks = inputs.Select(i => 
        context.ScheduleTask<int>(typeof(ProcessItemActivity), i)).ToList();
    
    // Fan-in: Wait for all to complete
    var results = await Task.WhenAll(tasks);
    
    return results;
}
```

### Human Interaction

```csharp
public override async Task<ApprovalResult> RunTask(
    OrchestrationContext context, 
    ApprovalRequest request)
{
    // Send notification to approver
    await context.ScheduleTask<bool>(typeof(SendApprovalRequestActivity), request);
    
    // Wait for approval with timeout
    using var cts = new CancellationTokenSource();
    var approvalTask = context.WaitForExternalEvent<bool>("Approved");
    var timeoutTask = context.CreateTimer(
        context.CurrentUtcDateTime.AddDays(7), 
        true, 
        cts.Token);
    
    var winner = await Task.WhenAny(approvalTask, timeoutTask);
    
    if (winner == approvalTask)
    {
        cts.Cancel();
        return new ApprovalResult { Approved = await approvalTask };
    }
    
    return new ApprovalResult { Approved = false, TimedOut = true };
}
```

### Monitor Pattern

```csharp
public override async Task<MonitorResult> RunTask(
    OrchestrationContext context, 
    MonitorInput input)
{
    int pollingInterval = 30; // seconds
    DateTime expiryTime = context.CurrentUtcDateTime.AddHours(2);
    
    while (context.CurrentUtcDateTime < expiryTime)
    {
        var status = await context.ScheduleTask<JobStatus>(
            typeof(CheckJobStatusActivity), 
            input.JobId);
        
        if (status.IsComplete)
        {
            return new MonitorResult { Completed = true, Status = status };
        }
        
        // Wait before polling again
        await context.CreateTimer(
            context.CurrentUtcDateTime.AddSeconds(pollingInterval), 
            true);
        
        // Optional: exponential backoff
        pollingInterval = Math.Min(pollingInterval * 2, 300);
    }
    
    return new MonitorResult { Completed = false, TimedOut = true };
}
```

> [!IMPORTANT]
>
> - Long loops can lead to resource exhaustion. Use `ContinueAsNew` for very long-running monitors.
> - Avoid tight polling loops; always include delays via `context.CreateTimer`.

## Getting Orchestration Information

### Current Instance ID

```csharp
string instanceId = context.OrchestrationInstance.InstanceId;
```

### Current Time

Always use `context.CurrentUtcDateTime` instead of `DateTime.UtcNow`:

```csharp
// ✅ Correct - deterministic
var now = context.CurrentUtcDateTime;

// ❌ Wrong - non-deterministic
var now = DateTime.UtcNow;
```

See [Deterministic Constraints](deterministic-constraints.md) for more details.

### Replay Detection

```csharp
if (!context.IsReplaying)
{
    // Only runs during first execution, not during replay
    _logger.LogInformation("Processing order {OrderId}", input.OrderId);
}
```

See [Replay and Durability](replay-and-durability.md) for more details.

## Starting Orchestrations

### From Client Code

```csharp
var client = new TaskHubClient(service, loggerFactory: loggerFactory);

// Start with auto-generated instance ID
var instance = await client.CreateOrchestrationInstanceAsync(
    typeof(OrderProcessingOrchestration),
    new OrderInput { OrderId = "12345" });

// Start with custom instance ID
var instance = await client.CreateOrchestrationInstanceAsync(
    typeof(OrderProcessingOrchestration),
    instanceId: "order-12345",
    input: new OrderInput { OrderId = "12345" });

// Start at a scheduled time
var instance = await client.CreateScheduledOrchestrationInstanceAsync(
    typeof(OrderProcessingOrchestration),
    instanceId: "scheduled-order",
    input: new OrderInput { OrderId = "12345" },
    startAt: DateTime.UtcNow.AddHours(1));
```

> [!NOTE]
> Not all backends support scheduled orchestrations.

### Waiting for Completion

```csharp
var result = await client.WaitForOrchestrationAsync(
    instance,
    timeout: TimeSpan.FromMinutes(5));

if (result.OrchestrationStatus == OrchestrationStatus.Completed)
{
    var output = result.Output; // Serialized result
}
```

## Error Handling

See [Error Handling](../features/error-handling.md) for comprehensive error handling patterns.

```csharp
public override async Task<string> RunTask(OrchestrationContext context, string input)
{
    try
    {
        return await context.ScheduleTask<string>(typeof(RiskyActivity), input);
    }
    catch (TaskFailedException ex)
    {
        // Activity threw an exception
        return await context.ScheduleTask<string>(typeof(CompensationActivity), input);
    }
}
```

## Next Steps

- [Activities](activities.md) — Writing activity code
- [Deterministic Constraints](deterministic-constraints.md) — Important rules for orchestration code
- [Replay and Durability](replay-and-durability.md) — Understanding how orchestrations are replayed
- [Features](../features/retries.md) — Retries, timers, events, and more
