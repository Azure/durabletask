# Orchestration Versioning

When you need to update orchestration code while instances are running, careful versioning strategies are required to avoid breaking in-flight orchestrations.

## The Versioning Problem

Orchestrations use [replay](../concepts/replay-and-durability.md) to rebuild state. If you change the code while an orchestration is in-flight, replay can fail.

### Example: Adding an Activity at the Beginning

```csharp
// Version 1
public override async Task<string> RunTask(OrchestrationContext context, string input)
{
    var a = await context.ScheduleTask<string>(typeof(ActivityA), input);
    var b = await context.ScheduleTask<string>(typeof(ActivityB), a);
    return b;
}

// Version 2 - Added a new activity at the BEGINNING
public override async Task<string> RunTask(OrchestrationContext context, string input)
{
    var validated = await context.ScheduleTask<string>(typeof(ValidateActivity), input);  // NEW
    var a = await context.ScheduleTask<string>(typeof(ActivityA), validated);
    var b = await context.ScheduleTask<string>(typeof(ActivityB), a);
    return b;
}
```

Suppose an instance started with V1 and completed `ActivityA`. Its history contains:

```text
TaskScheduled { Name: "ActivityA" }
TaskCompleted { Result: "..." }
```

When V2 code replays this history:

1. V2 expects first task to be `ValidateActivity`
2. History shows first task was `ActivityA`
3. **NonDeterministicOrchestrationException** is thrown

### Why Adding to the End Is Different

Adding activities at the **end** of an orchestration is generally safe because:

- Completed orchestrations are never replayed
- In-flight orchestrations haven't reached that point yet

```csharp
// Version 2 - Adding at the END is safe
public override async Task<string> RunTask(OrchestrationContext context, string input)
{
    var a = await context.ScheduleTask<string>(typeof(ActivityA), input);
    var b = await context.ScheduleTask<string>(typeof(ActivityB), a);
    var c = await context.ScheduleTask<string>(typeof(ActivityC), b);  // Usually safe to add here
    return c;
}
```

However, be cautious if in-flight orchestrations are waiting on timers or external events near the end—they may still replay and encounter the new code.

## Versioning Strategies

### Strategy 1: Side-by-Side Versioning

Deploy multiple versions of the orchestration simultaneously using `NameValueObjectCreator`:

```csharp
// Define both versions as separate classes
public class OrderOrchestrationV1 : TaskOrchestration<Result, Input>
{
    public override async Task<Result> RunTask(OrchestrationContext context, Input input)
    {
        // V1 logic
    }
}

public class OrderOrchestrationV2 : TaskOrchestration<Result, Input>
{
    public override async Task<Result> RunTask(OrchestrationContext context, Input input)
    {
        // V2 logic with new features
    }
}

// Register both with explicit name and version
worker.AddTaskOrchestrations(
    new NameValueObjectCreator<TaskOrchestration>(
        "OrderOrchestration", "V1", typeof(OrderOrchestrationV1)),
    new NameValueObjectCreator<TaskOrchestration>(
        "OrderOrchestration", "V2", typeof(OrderOrchestrationV2)));
```

Start new instances with the new version:

```csharp
// Start with specific version
var instance = await client.CreateOrchestrationInstanceAsync(
    "OrderOrchestration",
    "V2",  // Version string must match registration
    input);
```

### Strategy 2: Feature Flags with Version Check

Check version in orchestration code:

```csharp
public override async Task<Result> RunTask(OrchestrationContext context, Input input)
{
    var a = await context.ScheduleTask<string>(typeof(ActivityA), input);
    
    // Only run new code for instances started after cutoff
    if (context.OrchestrationInstance.ExecutionId != null &&
        input.Version >= 2)
    {
        var b = await context.ScheduleTask<string>(typeof(ActivityB), a);
        return new Result { Data = b };
    }
    
    return new Result { Data = a };
}
```

### Strategy 3: Wait for Completion

The safest approach for breaking changes:

1. **Stop starting new instances** of the old version
2. **Wait for all running instances** to complete
3. **Deploy the new version**
4. **Resume starting instances**

```csharp
// Query running instances
var runningInstances = await client.GetOrchestrationStateAsync(
    new OrchestrationStateQuery
    {
        RuntimeStatus = new[] { OrchestrationStatus.Running }
    });

// Wait for completion
while (runningInstances.Any())
{
    await Task.Delay(TimeSpan.FromMinutes(1));
    runningInstances = await client.GetOrchestrationStateAsync(...);
}

// Safe to deploy new version
```

### Strategy 4: Graceful Migration

For long-running orchestrations, add a migration point:

```csharp
// V1: Add migration check
public override async Task<Result> RunTask(OrchestrationContext context, Input input)
{
    // Check if migration is needed
    if (input.ShouldMigrate)
    {
        // Start V2 orchestration with current state
        var result = await context.CreateSubOrchestrationInstance<Result>(
            "OrderOrchestration",
            "2.0",
            input);
        return result;
    }
    
    // Continue with V1 logic
    var a = await context.ScheduleTask<string>(typeof(ActivityA), input);
    return new Result { Data = a };
}
```

### Strategy 5: Worker-Level Version Filtering

Configure workers to only process orchestrations matching specific version criteria using `VersioningSettings`. This enables zero-downtime deployments by running multiple worker versions simultaneously.

#### Setting Up VersioningSettings

```csharp
using DurableTask.Core.Settings;

var versioningSettings = new VersioningSettings
{
    Version = "2.0",
    MatchStrategy = VersioningSettings.VersionMatchStrategy.CurrentOrOlder,
    FailureStrategy = VersioningSettings.VersionFailureStrategy.Reject
};

var worker = new TaskHubWorker(orchestrationService, versioningSettings, loggerFactory);
```

> [!IMPORTANT]
> The `Version` property serves two purposes:
>
> 1. It defines which orchestrations this worker will process (based on `MatchStrategy`)
> 2. It becomes the **default version** for all new orchestrations created without an explicit version

This means when you start a new orchestration without specifying a version, it will automatically be stamped with the worker's configured version:

```csharp
// This orchestration will be created with version "2.0" (from VersioningSettings)
var instance = await client.CreateOrchestrationInstanceAsync(
    typeof(OrderOrchestration),
    input);
```

#### Version Match Strategies

| Strategy | Description |
| -------- | ----------- |
| `None` | Default. Ignore version, process all orchestrations. |
| `Strict` | Only process orchestrations with an **exact** version match. |
| `CurrentOrOlder` | Process orchestrations with version **less than or equal** to the worker version. |

#### Version Failure Strategies

| Strategy | Description |
| -------- | ----------- |
| `Reject` | Default. Abandon the work item so another worker can pick it up (or retry later). |
| `Fail` | Fail the orchestration with a `VersionMismatch` error. |

#### Blue-Green Deployment Example

Run old and new workers simultaneously during deployments:

```csharp
// OLD worker (handles existing orchestrations)
var oldSettings = new VersioningSettings
{
    Version = "1.0",
    MatchStrategy = VersioningSettings.VersionMatchStrategy.Strict,
    FailureStrategy = VersioningSettings.VersionFailureStrategy.Reject
};
var oldWorker = new TaskHubWorker(orchestrationService, oldSettings, loggerFactory);
oldWorker.AddTaskOrchestrations(typeof(OrderOrchestrationV1));

// NEW worker (handles new orchestrations)
var newSettings = new VersioningSettings
{
    Version = "2.0",
    MatchStrategy = VersioningSettings.VersionMatchStrategy.Strict,
    FailureStrategy = VersioningSettings.VersionFailureStrategy.Reject
};
var newWorker = new TaskHubWorker(orchestrationService, newSettings, loggerFactory);
newWorker.AddTaskOrchestrations(typeof(OrderOrchestrationV2));

// Both workers run simultaneously
// - V1 orchestrations are processed by oldWorker
// - V2 orchestrations are processed by newWorker
// Once all V1 orchestrations complete, retire oldWorker
```

#### Version Comparison

Versions are compared using the following rules:

1. Empty versions are treated as "unversioned" and compare as less than any defined version
2. If both versions can be parsed as `System.Version` (e.g., "1.0.0", "2.1"), numeric comparison is used
3. Otherwise, case-insensitive string comparison is used

```csharp
// Version comparison examples
VersioningSettings.CompareVersions("1.0.0", "1.0.0");  // Returns 0 (equal)
VersioningSettings.CompareVersions("2.0.0", "1.0.0");  // Returns 1 (greater)
VersioningSettings.CompareVersions("1.0.0", "2.0.0");  // Returns -1 (less)
VersioningSettings.CompareVersions("", "1.0.0");       // Returns -1 (empty < defined)
```

#### Accessing Version in Orchestrations

The orchestration version is available via `OrchestrationContext.Version`:

```csharp
public override async Task<Result> RunTask(OrchestrationContext context, Input input)
{
    // Access the version this orchestration was started with
    string version = context.Version;
    
    if (!context.IsReplaying)
    {
        _logger.LogInformation("Processing orchestration version: {Version}", version);
    }
    
    // Use version for conditional logic (CompareVersions handles "2.0", "2.1", "3.0", etc.)
    if (VersioningSettings.CompareVersions(version, "2.0") >= 0)
    {
        // V2+ specific logic
    }
    
    // ...
}
```

## Safe Code Changes

### Changes That Are Safe

✅ **Adding activities at the end** (after all existing durable operations):

```csharp
// Safe - existing orchestrations completed or haven't reached this point
var a = await context.ScheduleTask<string>(typeof(ActivityA), input);
var b = await context.ScheduleTask<string>(typeof(ActivityB), a);
var c = await context.ScheduleTask<string>(typeof(ActivityC), b);  // Added at end
```

✅ **Changing activity implementation** (not the orchestration code):

```csharp
// Safe - activity logic doesn't affect replay
public class ActivityA : TaskActivity<string, string>
{
    protected override string Execute(TaskContext context, string input)
    {
        return input.ToUpper();  // Changed from ToLower()
    }
}
```

✅ **Adding logging or metrics** (using IsReplaying):

```csharp
if (!context.IsReplaying)
{
    _logger.LogInformation("Processing...");  // Safe to add
}
```

✅ **Changing non-durable code**:

```csharp
var formatted = input.Trim().ToLower();  // Safe to change
var result = await context.ScheduleTask<string>(typeof(MyActivity), formatted);
```

### Changes That Are NOT Safe

❌ **Removing or reordering activities**:

```csharp
// V1
var a = await context.ScheduleTask<string>(typeof(ActivityA), input);
var b = await context.ScheduleTask<string>(typeof(ActivityB), a);

// V2 - BREAKS replay
var b = await context.ScheduleTask<string>(typeof(ActivityB), input);
var a = await context.ScheduleTask<string>(typeof(ActivityA), b);
```

❌ **Changing activity types**:

```csharp
// V1
await context.ScheduleTask<string>(typeof(ActivityA), input);

// V2 - BREAKS replay (different activity name)
await context.ScheduleTask<string>(typeof(ActivityANew), input);
```

❌ **Changing conditional logic that affects scheduling**:

```csharp
// V1
if (input.Amount > 100)
    await context.ScheduleTask<string>(typeof(LargeOrderActivity), input);

// V2 - BREAKS replay (different threshold)
if (input.Amount > 50)  // Changed condition!
    await context.ScheduleTask<string>(typeof(LargeOrderActivity), input);
```

❌ **Adding activities in the middle**:

```csharp
// V1
var a = await context.ScheduleTask<string>(typeof(ActivityA), input);
var c = await context.ScheduleTask<string>(typeof(ActivityC), a);

// V2 - BREAKS replay
var a = await context.ScheduleTask<string>(typeof(ActivityA), input);
var b = await context.ScheduleTask<string>(typeof(ActivityB), a);  // Added in middle!
var c = await context.ScheduleTask<string>(typeof(ActivityC), b);
```

❌ **Changing retry policies**:

```csharp
// V1
var options = new RetryOptions(TimeSpan.FromSeconds(5), maxNumberOfAttempts: 3);
await context.ScheduleWithRetry<string>(typeof(ActivityA), options, input);

// V2 - BREAKS replay (different retry behavior recorded in history)
var options = new RetryOptions(TimeSpan.FromSeconds(10), maxNumberOfAttempts: 5);
await context.ScheduleWithRetry<string>(typeof(ActivityA), options, input);
```

## Orchestration Name Registration

### Custom Naming

By default, orchestrations are registered using their class name. Use `NameValueObjectCreator` to specify a custom name:

```csharp
public class OrderOrchestration : TaskOrchestration<Result, Input> { }

// Register with custom name "OrderProcessing" instead of class name
worker.AddTaskOrchestrations(
    new NameValueObjectCreator<TaskOrchestration>(
        "OrderProcessing", "", typeof(OrderOrchestration)));
```

### Side-by-Side Registration

Use `NameValueObjectCreator` to register multiple versions of the same orchestration:

```csharp
public class OrderOrchestrationV1 : TaskOrchestration<Result, Input> { /* V1 impl */ }

public class OrderOrchestrationV2 : TaskOrchestration<Result, Input> { /* V2 impl */ }
```

### Registration

```csharp
worker.AddTaskOrchestrations(
    new NameValueObjectCreator<TaskOrchestration>(
        "OrderProcessing", 
        "V1", 
        typeof(OrderOrchestrationV1)),
    new NameValueObjectCreator<TaskOrchestration>(
        "OrderProcessing", 
        "V2", 
        typeof(OrderOrchestrationV2)));
```

## Best Practices

### 1. Plan for Versioning from the Start

```csharp
public class OrderInput
{
    public int Version { get; set; } = 1;  // Include version in input
    public string OrderId { get; set; }
    // ...
}
```

### 2. Use Feature Flags for Gradual Rollout

```csharp
public override async Task<Result> RunTask(OrchestrationContext context, Input input)
{
    if (input.Features.UseNewPaymentFlow)
    {
        return await NewPaymentFlowAsync(context, input);
    }
    return await LegacyPaymentFlowAsync(context, input);
}
```

### 3. Keep Orchestrations Short-Lived When Possible

Long-running orchestrations are harder to version. Consider:

- Breaking into sub-orchestrations
- Using `ContinueAsNew` more frequently
- Designing for completion within hours/days, not months

### 4. Document Breaking Changes

```csharp
/// <summary>
/// Order processing orchestration.
/// 
/// Version History:
/// - V1: Initial version
/// - V2: Added fraud check activity (BREAKING - wait for V1 completion)
/// - V2.1: Updated logging (compatible with V2)
/// </summary>
public class OrderOrchestrationV2_1 : TaskOrchestration<Result, Input> { }

// Register with name and version
worker.AddTaskOrchestrations(
    new NameValueObjectCreator<TaskOrchestration>(
        "OrderProcessing", "V2.1", typeof(OrderOrchestrationV2_1)));
```

## Next Steps

- [Replay and Durability](../concepts/replay-and-durability.md) — Understanding why versioning matters
- [Deterministic Constraints](../concepts/deterministic-constraints.md) — Writing safe orchestration code
- [Error Handling](error-handling.md) — Handling version mismatch errors
