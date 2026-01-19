# DurableTask.Samples

This project contains core sample orchestrations demonstrating fundamental patterns of the Durable Task Framework using the Azure Storage backend.

## Prerequisites

- .NET Framework 4.8 or later
- Azure Storage Emulator (Azurite) or Azure Storage account

## Configuration

Configure the connection string in `App.config`:

```xml
<appSettings>
  <add key="StorageConnectionString" value="UseDevelopmentStorage=true" />
  <add key="taskHubName" value="SamplesHub" />
</appSettings>
```

For Azure Storage, replace `UseDevelopmentStorage=true` with your connection string (if not using the emulator):

```text
DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...
```

## Running the Samples

### 1. Create the Task Hub (first time only)

```bash
DurableTask.Samples.exe -c
```

### 2. Start an Orchestration

```bash
DurableTask.Samples.exe -s <OrchestrationName> [-p <parameters>]
```

The worker automatically starts and waits for the orchestration to complete.

## Available Samples

### Greetings

A simple "Hello World" orchestration that calls greeting activities.

```csharp
public class GreetingsOrchestration : TaskOrchestration<string, string>
{
    public override async Task<string> RunTask(OrchestrationContext context, string input)
    {
        string greeting = await context.ScheduleTask<string>(typeof(GetUserTask));
        string result = await context.ScheduleTask<string>(typeof(SendGreetingTask), greeting);
        return result;
    }
}
```

**Run:** `DurableTask.Samples.exe -s Greetings`

### Greetings2

Demonstrates parameterized orchestrations with a configurable number of greetings.

**Run:** `DurableTask.Samples.exe -s Greetings2 -p 5`

### Cron

An eternal orchestration that runs on a schedule using `CreateTimer` and `ContinueAsNew`.

```csharp
public class CronOrchestration : TaskOrchestration<string, string>
{
    public override async Task<string> RunTask(OrchestrationContext context, string schedule)
    {
        // Execute the scheduled task
        await context.ScheduleTask<string>(typeof(CronTask));
        
        // Wait until next scheduled time
        DateTime nextRun = CalculateNextRun(context.CurrentUtcDateTime, schedule);
        await context.CreateTimer(nextRun, true);
        
        // Continue as new instance
        context.ContinueAsNew(schedule);
        return "Completed cycle";
    }
}
```

**Run:** `DurableTask.Samples.exe -s Cron -p "0 12 * * *"`

### AverageCalculator

Fan-out/fan-in pattern that distributes computation across multiple activities.

```csharp
public class AverageCalculatorOrchestration : TaskOrchestration<double, int[]>
{
    public override async Task<double> RunTask(OrchestrationContext context, int[] numbers)
    {
        // Fan-out: process chunks in parallel
        var tasks = new List<Task<int>>();
        foreach (var chunk in numbers.Chunk(10))
        {
            tasks.Add(context.ScheduleTask<int>(typeof(ComputeSumTask), chunk));
        }
        
        // Fan-in: aggregate results
        int[] sums = await Task.WhenAll(tasks);
        return sums.Sum() / (double)numbers.Length;
    }
}
```

**Run:** `DurableTask.Samples.exe -s Average -p "1 50 10"`

Parameters: `<start> <end> <chunkSize>`

### ErrorHandling

Demonstrates retry policies and exception handling patterns.

```csharp
public override async Task<string> RunTask(OrchestrationContext context, string input)
{
    var retryOptions = new RetryOptions(
        firstRetryInterval: TimeSpan.FromSeconds(5),
        maxNumberOfAttempts: 3);
    
    try
    {
        return await context.ScheduleWithRetry<string>(
            typeof(UnreliableActivity), 
            retryOptions, 
            input);
    }
    catch (TaskFailedException ex)
    {
        // Handle permanent failure
        return $"Failed after retries: {ex.Message}";
    }
}
```

**Run:** `DurableTask.Samples.exe -s ErrorHandling`

### Signal

Demonstrates external events and human interaction patterns.

```csharp
public override async Task<string> RunTask(OrchestrationContext context, ApprovalRequest input)
{
    // Send notification
    await context.ScheduleTask<bool>(typeof(SendApprovalRequest), input);
    
    // Wait for external event
    var approval = await context.WaitForExternalEvent<ApprovalResult>("ApprovalResult");
    
    if (approval.IsApproved)
    {
        await context.ScheduleTask<bool>(typeof(ProcessApproval), input);
        return "Approved and processed";
    }
    
    return "Rejected";
}
```

**Run:** `DurableTask.Samples.exe -s Signal`

To raise an event to a running instance:

```bash
DurableTask.Samples.exe -n <eventName> -i <instanceId> -p <eventData>
```

### SumOfSquares

Another fan-out/fan-in example computing sum of squares from a JSON input file.

**Run:** `DurableTask.Samples.exe -s SumOfSquares`

## Command Line Options

| Option | Description |
| ------ | ----------- |
| `-c` | Create the task hub (required on first run) |
| `-s <name>` | Start the specified orchestration |
| `-p <params>` | Parameters to pass to the orchestration |
| `-i <id>` | Instance ID (auto-generated if not specified) |
| `-n <name>` | Event name for raising events |
| `-w` | Skip the worker (useful when worker runs separately) |

## Additional Resources

- [Getting Started Guide](../../docs/getting-started/quickstart.md)
- [Orchestrations](../../docs/concepts/orchestrations.md)
- [Activities](../../docs/concepts/activities.md)
- [Error Handling](../../docs/features/error-handling.md)
- [Timers](../../docs/features/timers.md)
