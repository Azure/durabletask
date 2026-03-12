# Durable Timers

Durable timers allow orchestrations to wait for specified durations or until specific times. Unlike `Thread.Sleep` or `Task.Delay`, durable timers are persisted and survive process restarts.

## Creating Timers

### Wait for Duration

```csharp
// Wait for 5 minutes
await context.CreateTimer(
    context.CurrentUtcDateTime.AddMinutes(5), 
    true);
```

### Wait Until Specific Time

```csharp
// Wait until midnight
var midnight = context.CurrentUtcDateTime.Date.AddDays(1);
await context.CreateTimer(midnight, true);
```

### Timer with CancellationToken

```csharp
using var cts = new CancellationTokenSource();
var timerTask = context.CreateTimer(
    context.CurrentUtcDateTime.AddHours(1),
    true,
    cts.Token);

// Cancel the timer if needed
cts.Cancel();
```

## Common Patterns

### Timeout with Fallback

```csharp
public override async Task<Result> RunTask(OrchestrationContext context, Input input)
{
    using var cts = new CancellationTokenSource();
    
    var workTask = context.ScheduleTask<Result>(typeof(LongRunningActivity), input);
    var timeoutTask = context.CreateTimer(
        context.CurrentUtcDateTime.AddMinutes(30),
        true,
        cts.Token);
    
    var winner = await Task.WhenAny(workTask, timeoutTask);
    
    if (winner == workTask)
    {
        cts.Cancel();  // Cancel the timer
        return await workTask;
    }
    else
    {
        // Timeout occurred
        return new Result { TimedOut = true };
    }
}
```

### Approval with Deadline

```csharp
public override async Task<ApprovalResult> RunTask(
    OrchestrationContext context, 
    ApprovalRequest request)
{
    // Send approval request
    await context.ScheduleTask<bool>(typeof(SendApprovalEmail), request);
    
    using var cts = new CancellationTokenSource();
    
    // Wait for approval event or 7-day timeout
    var approvalTask = context.WaitForExternalEvent<bool>("Approved");
    var deadlineTask = context.CreateTimer(
        context.CurrentUtcDateTime.AddDays(7),
        true,
        cts.Token);
    
    var winner = await Task.WhenAny(approvalTask, deadlineTask);
    
    if (winner == approvalTask)
    {
        cts.Cancel();
        var approved = await approvalTask;
        return new ApprovalResult { Approved = approved };
    }
    else
    {
        return new ApprovalResult { Approved = false, Expired = true };
    }
}
```

### Periodic Polling (Monitor Pattern)

```csharp
public override async Task<JobResult> RunTask(
    OrchestrationContext context, 
    JobInput input)
{
    var expirationTime = context.CurrentUtcDateTime.AddHours(4);
    var pollingInterval = TimeSpan.FromSeconds(30);
    
    while (context.CurrentUtcDateTime < expirationTime)
    {
        var status = await context.ScheduleTask<JobStatus>(
            typeof(CheckJobStatusActivity),
            input.JobId);
        
        if (status.IsComplete)
        {
            return new JobResult { Success = true, Data = status.Data };
        }
        
        // Wait before next poll
        var nextCheck = context.CurrentUtcDateTime.Add(pollingInterval);
        await context.CreateTimer(nextCheck, true);
        
        // Optional: exponential backoff
        pollingInterval = TimeSpan.FromSeconds(
            Math.Min(pollingInterval.TotalSeconds * 1.5, 300));
    }
    
    return new JobResult { Success = false, TimedOut = true };
}
```

### Scheduled Execution

```csharp
public override async Task<string> RunTask(
    OrchestrationContext context, 
    ScheduledTask input)
{
    // Wait until scheduled time
    if (context.CurrentUtcDateTime < input.ScheduledTime)
    {
        await context.CreateTimer(input.ScheduledTime, true);
    }
    
    // Execute the task
    return await context.ScheduleTask<string>(
        typeof(ScheduledWorkActivity),
        input);
}
```

### Cron-like Scheduling

```csharp
public override async Task RunTask(OrchestrationContext context, CronInput input)
{
    var nextRun = GetNextCronTime(input.CronExpression, context.CurrentUtcDateTime);
    
    // Wait until next scheduled time
    await context.CreateTimer(nextRun, true);
    
    // Execute scheduled work
    await context.ScheduleTask<bool>(typeof(CronJobActivity), input);
    
    // Continue as new for next iteration
    context.ContinueAsNew(input);
}

private DateTime GetNextCronTime(string cronExpression, DateTime fromTime)
{
    // Use a cron parsing library like Cronos
    var expression = CronExpression.Parse(cronExpression);
    return expression.GetNextOccurrence(fromTime, TimeZoneInfo.Utc) 
           ?? throw new InvalidOperationException("No next occurrence");
}
```

### Reminder/Notification Pattern

```csharp
public override async Task RunTask(OrchestrationContext context, ReminderInput input)
{
    // Send initial notification
    await context.ScheduleTask<bool>(typeof(SendReminderActivity), new ReminderData
    {
        UserId = input.UserId,
        Message = input.InitialMessage
    });
    
    // Send follow-up reminders
    foreach (var reminder in input.FollowUpSchedule)
    {
        await context.CreateTimer(
            context.CurrentUtcDateTime.Add(reminder.Delay),
            true);
        
        await context.ScheduleTask<bool>(typeof(SendReminderActivity), new ReminderData
        {
            UserId = input.UserId,
            Message = reminder.Message
        });
    }
}
```

## Timer Behavior

### Durability

Timers are persisted as `TimerCreated` events:

```text
1. ExecutionStarted
2. TimerCreated { FireAt: "2024-01-15T10:00:00Z" }
```

When the timer fires, a `TimerFired` event is added:

```text
3. TimerFired { TimerId: 1 }
```

### Replay Behavior

During replay:

- Past timers complete immediately (fire time already passed)
- Future timers wait for the scheduled time

### Minimum Duration

Very short timers (< 1 second) may not provide precise timing due to:

- Message processing overhead
- Partition lease renewal intervals
- Clock synchronization

For precise short delays, use activities.

## Best Practices

### 1. Always Use Context Time

```csharp
// ✅ Correct
await context.CreateTimer(context.CurrentUtcDateTime.AddMinutes(5), true);

// ❌ Wrong - non-deterministic
await context.CreateTimer(DateTime.UtcNow.AddMinutes(5), true);
```

### 2. Cancel Unused Timers

```csharp
using var cts = new CancellationTokenSource();
var timer = context.CreateTimer(deadline, true, cts.Token);
var work = context.WaitForExternalEvent<string>("Event");

var winner = await Task.WhenAny(timer, work);
if (winner == work)
{
    cts.Cancel();  // Important: cancel the timer
}
```

> [!NOTE]
> If an orchestration completes while timers are pending, the orchestration will remain in the "Running" state until all timers either fire or are cancelled.

### 3. Avoid Very Long Timers Without ContinueAsNew

Super long timers make it harder to version orchestration code. Periodically break up long waits using `ContinueAsNew` if possible.

```csharp
// For very long waits, consider breaking up with ContinueAsNew
public override async Task RunTask(OrchestrationContext context, WaitInput input)
{
    var remainingWait = input.TotalWait - (context.CurrentUtcDateTime - input.StartTime);
    
    if (remainingWait > TimeSpan.FromDays(7))
    {
        // Wait for a week, then continue as new
        await context.CreateTimer(
            context.CurrentUtcDateTime.AddDays(7),
            true);
        context.ContinueAsNew(input);
        return;
    }
    
    await context.CreateTimer(
        context.CurrentUtcDateTime.Add(remainingWait),
        true);
    
    await context.ScheduleTask<bool>(typeof(FinalActivity), input);
}
```

## Next Steps

- [External Events](external-events.md) — Combining timers with events
- [Eternal Orchestrations](eternal-orchestrations.md) — Long-running workflows
- [Replay and Durability](../concepts/replay-and-durability.md) — How timers are persisted
