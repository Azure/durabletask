# Middleware

Middleware in the Durable Task Framework allows you to intercept and extend orchestration and activity execution. This is useful for cross-cutting concerns like logging, metrics, authentication, or context propagation.

## Middleware Delegate Signature

Middleware is registered as a delegate with the following signature:

```csharp
using DurableTask.Core.Middleware;

// Middleware delegate signature
Func<DispatchMiddlewareContext, Func<Task>, Task>
```

The `DispatchMiddlewareContext` provides access to execution context via `GetProperty<T>()` and `SetProperty<T>()` methods.

## Orchestration Middleware

### Available Context Properties

Orchestration middleware can access these properties via `context.GetProperty<T>()`:

| Type | Description |
| ---- | ----------- |
| `OrchestrationInstance` | The orchestration instance (InstanceId, ExecutionId) |
| `TaskOrchestration` | The orchestration implementation (may be null for out-of-process scenarios) |
| `OrchestrationRuntimeState` | History, status, name, version, input, tags, and more |
| `OrchestrationExecutionContext` | Contains orchestration tags |
| `TaskOrchestrationWorkItem` | The work item being processed |

### Creating Orchestration Middleware

```csharp
public static class OrchestrationLoggingMiddleware
{
    public static Func<DispatchMiddlewareContext, Func<Task>, Task> Create(ILogger logger)
    {
        return async (context, next) =>
        {
            var instance = context.GetProperty<OrchestrationInstance>();
            var runtimeState = context.GetProperty<OrchestrationRuntimeState>();
            var instanceId = instance?.InstanceId ?? "unknown";
            var orchestrationName = runtimeState?.Name ?? "unknown";
            
            logger.LogInformation("Orchestration {Name} ({InstanceId}) starting execution", 
                orchestrationName, instanceId);
            var stopwatch = Stopwatch.StartNew();
            
            try
            {
                await next();
                logger.LogInformation("Orchestration {Name} ({InstanceId}) completed in {ElapsedMs}ms", 
                    orchestrationName, instanceId, stopwatch.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Orchestration {Name} ({InstanceId}) failed after {ElapsedMs}ms", 
                    orchestrationName, instanceId, stopwatch.ElapsedMilliseconds);
                throw;
            }
        };
    }
}
```

### Registering Orchestration Middleware

```csharp
var worker = new TaskHubWorker(orchestrationService, loggerFactory);

// Add middleware using lambda - order matters (first registered = outermost)
worker.AddOrchestrationDispatcherMiddleware(async (context, next) =>
{
    var instance = context.GetProperty<OrchestrationInstance>();
    Console.WriteLine($"Processing orchestration: {instance?.InstanceId}");
    await next();
});

// Or use a factory method
worker.AddOrchestrationDispatcherMiddleware(
    OrchestrationLoggingMiddleware.Create(logger));

await worker.StartAsync();
```

## Activity Middleware

### Context Properties for Activities

Activity middleware can access these properties via `context.GetProperty<T>()`:

| Type | Description |
| ---- | ----------- |
| `OrchestrationInstance` | The parent orchestration instance |
| `TaskActivity` | The activity implementation (may be null for out-of-process scenarios) |
| `TaskScheduledEvent` | Contains activity name, version, input, and event ID |
| `OrchestrationExecutionContext` | Contains orchestration tags (if available) |

### Creating Activity Middleware

```csharp
public static class ActivityLoggingMiddleware
{
    public static Func<DispatchMiddlewareContext, Func<Task>, Task> Create(ILogger logger)
    {
        return async (context, next) =>
        {
            var scheduledEvent = context.GetProperty<TaskScheduledEvent>();
            var instance = context.GetProperty<OrchestrationInstance>();
            var activityName = scheduledEvent?.Name ?? "unknown";
            var instanceId = instance?.InstanceId ?? "unknown";
            
            logger.LogInformation("Activity {ActivityName} starting for orchestration {InstanceId}", 
                activityName, instanceId);
            var stopwatch = Stopwatch.StartNew();
            
            try
            {
                await next();
                logger.LogInformation("Activity {ActivityName} completed in {ElapsedMs}ms", 
                    activityName, stopwatch.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Activity {ActivityName} failed after {ElapsedMs}ms", 
                    activityName, stopwatch.ElapsedMilliseconds);
                throw;
            }
        };
    }
}
```

### Registering Activity Middleware

```csharp
var worker = new TaskHubWorker(orchestrationService, loggerFactory);

// Add middleware using lambda
worker.AddActivityDispatcherMiddleware(async (context, next) =>
{
    var scheduledEvent = context.GetProperty<TaskScheduledEvent>();
    Console.WriteLine($"Executing activity: {scheduledEvent?.Name}");
    await next();
});

// Or use a factory method
worker.AddActivityDispatcherMiddleware(
    ActivityLoggingMiddleware.Create(logger));

await worker.StartAsync();
```

## Common Middleware Patterns

### Metrics Collection

```csharp
worker.AddOrchestrationDispatcherMiddleware(async (context, next) =>
{
    var runtimeState = context.GetProperty<OrchestrationRuntimeState>();
    var orchestrationName = runtimeState?.Name ?? "unknown";
    var stopwatch = Stopwatch.StartNew();
    var success = true;
    
    try
    {
        await next();
    }
    catch
    {
        success = false;
        throw;
    }
    finally
    {
        metrics.RecordDuration($"orchestration.{orchestrationName}.duration", stopwatch.Elapsed);
        metrics.RecordCounter(success ? "orchestration.success" : "orchestration.failure");
    }
});
```

### Context Propagation (Using Tags)

```csharp
worker.AddOrchestrationDispatcherMiddleware(async (context, next) =>
{
    var executionContext = context.GetProperty<OrchestrationExecutionContext>();
    
    // Extract tenant ID from orchestration tags
    string tenantId = "default";
    if (executionContext?.OrchestrationTags?.TryGetValue("TenantId", out var tenant) == true)
    {
        tenantId = tenant;
    }
    
    // Set ambient context
    using (TenantContext.SetCurrent(tenantId))
    {
        await next();
    }
});
```

### Exception Handling Considerations

> [!IMPORTANT]
> Exceptions thrown in middleware cause the work item to be **retried**, not failed. If you want to explicitly fail an orchestration or activity, you must set the result directly.

```csharp
// CAUTION: This causes infinite retries, NOT a failure!
worker.AddActivityDispatcherMiddleware(async (context, next) =>
{
    try
    {
        await next();
    }
    catch (Exception ex)
    {
        // Logging is fine, but re-throwing will cause retries
        logger.LogError(ex, "Activity failed");
        throw;  // ⚠️ This causes the activity to be retried, not failed!
    }
});
```

To properly fail an activity from middleware, use `TaskFailureException` or set the result:

```csharp
// Option 1: Throw TaskFailureException (gets converted to TaskFailedEvent)
worker.AddActivityDispatcherMiddleware(async (context, next) =>
{
    try
    {
        await next();
    }
    catch (Exception ex)
    {
        // This properly fails the activity and reports failure to the orchestration
        throw new TaskFailureException(ex.Message, ex, ex.ToString());
    }
});

// Option 2: Set the failure result directly
worker.AddActivityDispatcherMiddleware(async (context, next) =>
{
    var scheduledEvent = context.GetProperty<TaskScheduledEvent>();
    
    try
    {
        await next();
    }
    catch (Exception ex)
    {
        // Explicitly set a failure result
        context.SetProperty(new ActivityExecutionResult
        {
            ResponseEvent = new TaskFailedEvent(
                eventId: -1,
                taskScheduledEventId: scheduledEvent.EventId,
                reason: ex.Message,
                details: ex.ToString(),
                failureDetails: new FailureDetails(ex))
        });
        // Don't re-throw - we've handled the failure
    }
});
```

### Authentication/Authorization

```csharp
worker.AddOrchestrationDispatcherMiddleware(async (context, next) =>
{
    var executionContext = context.GetProperty<OrchestrationExecutionContext>();
    
    string? userId = null;
    executionContext?.OrchestrationTags?.TryGetValue("UserId", out userId);
    
    if (string.IsNullOrEmpty(userId) || 
        !await authService.IsAuthorizedAsync(userId, "ExecuteOrchestration"))
    {
        // Don't throw - that would cause retries. Instead, fail the orchestration explicitly.
        context.SetProperty(new OrchestratorExecutionResult
        {
            Actions = new[]
            {
                new OrchestrationCompleteOrchestratorAction
                {
                    OrchestrationStatus = OrchestrationStatus.Failed,
                    Result = $"User {userId ?? "unknown"} is not authorized to execute orchestrations",
                    FailureDetails = new FailureDetails(
                        errorType: "UnauthorizedAccessException",
                        errorMessage: $"User {userId ?? "unknown"} is not authorized",
                        stackTrace: null,
                        innerFailure: null,
                        isNonRetriable: true)
                }
            }
        });
        return; // Don't call next()
    }
    
    await next();
});
```

## Middleware Context

### Accessing Built-in Properties

```csharp
// For orchestration middleware
worker.AddOrchestrationDispatcherMiddleware(async (context, next) =>
{
    // Core identification
    var instance = context.GetProperty<OrchestrationInstance>();
    var instanceId = instance?.InstanceId;
    var executionId = instance?.ExecutionId;
    
    // Orchestration metadata
    var runtimeState = context.GetProperty<OrchestrationRuntimeState>();
    var orchestrationName = runtimeState?.Name;
    var orchestrationVersion = runtimeState?.Version;
    var input = runtimeState?.Input;
    var status = runtimeState?.OrchestrationStatus;
    
    // Tags
    var executionContext = context.GetProperty<OrchestrationExecutionContext>();
    var tags = executionContext?.OrchestrationTags;
    
    // The orchestration implementation (may be null for out-of-process execution)
    var orchestration = context.GetProperty<TaskOrchestration>();
    
    await next();
});

// For activity middleware
worker.AddActivityDispatcherMiddleware(async (context, next) =>
{
    // Parent orchestration instance
    var instance = context.GetProperty<OrchestrationInstance>();
    
    // Activity details from the scheduled event
    var scheduledEvent = context.GetProperty<TaskScheduledEvent>();
    var activityName = scheduledEvent?.Name;
    var activityVersion = scheduledEvent?.Version;
    var activityInput = scheduledEvent?.Input;
    var eventId = scheduledEvent?.EventId;
    
    // The activity implementation (may be null for out-of-process execution)
    var activity = context.GetProperty<TaskActivity>();
    
    await next();
});
```

### Setting Custom Properties

```csharp
// First middleware sets a property
worker.AddOrchestrationDispatcherMiddleware(async (context, next) =>
{
    // Set a named property for downstream middleware
    context.SetProperty("CorrelationId", Guid.NewGuid().ToString());
    await next();
});

// Downstream middleware reads the property
worker.AddOrchestrationDispatcherMiddleware(async (context, next) =>
{
    var correlationId = context.GetProperty<string>("CorrelationId");
    Console.WriteLine($"Correlation ID: {correlationId}");
    await next();
});
```

## Middleware Ordering

Middleware executes in a pipeline. The order of registration determines execution order:

```csharp
// Registration order
worker.AddOrchestrationDispatcherMiddleware(AuthMiddleware);      // 1st registered
worker.AddOrchestrationDispatcherMiddleware(LoggingMiddleware);   // 2nd registered
worker.AddOrchestrationDispatcherMiddleware(MetricsMiddleware);   // 3rd registered

// Execution order (onion model):
// AuthMiddleware →
//   LoggingMiddleware →
//     MetricsMiddleware →
//       [Orchestration executes]
//     ← MetricsMiddleware returns
//   ← LoggingMiddleware returns
// ← AuthMiddleware returns
```

## Best Practices

### 1. Keep Middleware Focused

Each middleware should have a single responsibility:

```csharp
// Good - single responsibility with factory methods
public static class LoggingMiddleware
{
    public static Func<DispatchMiddlewareContext, Func<Task>, Task> Create(ILogger logger) => /* logging only */;
}

public static class MetricsMiddleware
{
    public static Func<DispatchMiddlewareContext, Func<Task>, Task> Create(IMetrics metrics) => /* metrics only */;
}

// Avoid combining multiple concerns in one middleware
```

### 2. Understand Exception Behavior

Exceptions thrown in middleware cause **retries**, not failures:

```csharp
// For activities: Use TaskFailureException to signal failure to orchestration
worker.AddActivityDispatcherMiddleware(async (context, next) =>
{
    try
    {
        await next();
    }
    catch (MyValidationException ex)
    {
        // Convert to TaskFailureException to properly fail the activity
        throw new TaskFailureException(ex.Message, ex, ex.ToString());
    }
    // Other exceptions will cause retries
});

// For orchestrations: Set result with failed status
worker.AddOrchestrationDispatcherMiddleware(async (context, next) =>
{
    try
    {
        await next();
    }
    catch (Exception ex) when (ShouldFailOrchestration(ex))
    {
        context.SetProperty(new OrchestratorExecutionResult
        {
            Actions = new[]
            {
                new OrchestrationCompleteOrchestratorAction
                {
                    OrchestrationStatus = OrchestrationStatus.Failed,
                    Result = ex.Message,
                    FailureDetails = new FailureDetails(ex)
                }
            }
        });
        // Don't re-throw - we've handled the failure
    }
});
```

### 3. Use Dependency Injection Patterns

Capture dependencies via closures or factory methods:

```csharp
// Using closures
public static Func<DispatchMiddlewareContext, Func<Task>, Task> CreateTelemetryMiddleware(
    TelemetryClient telemetry,
    ILogger logger)
{
    return async (context, next) =>
    {
        var instance = context.GetProperty<OrchestrationInstance>();
        telemetry.TrackEvent("OrchestrationStarted", 
            new Dictionary<string, string> { ["InstanceId"] = instance?.InstanceId });
        
        await next();
    };
}

// Registration
worker.AddOrchestrationDispatcherMiddleware(
    CreateTelemetryMiddleware(telemetryClient, logger));
```

### 4. Intercepting Execution Results

Middleware can intercept and modify execution results:

```csharp
// For orchestrations - intercept or provide custom results
worker.AddOrchestrationDispatcherMiddleware(async (context, next) =>
{
    await next();
    
    // After execution, you can read the result
    var result = context.GetProperty<OrchestratorExecutionResult>();
    // Inspect result.Actions, result.CustomStatus, etc.
});

// For activities - intercept or provide custom results
worker.AddActivityDispatcherMiddleware(async (context, next) =>
{
    await next();
    
    // After execution, you can read the result
    var result = context.GetProperty<ActivityExecutionResult>();
    // Inspect result.ResponseEvent
});
```

### 5. Out-of-Process Execution

Middleware can completely replace execution for out-of-process scenarios:

```csharp
worker.AddOrchestrationDispatcherMiddleware(async (context, next) =>
{
    var runtimeState = context.GetProperty<OrchestrationRuntimeState>();
    
    // Execute orchestration out-of-process and get result
    var actions = await ExecuteOutOfProcessAsync(runtimeState);
    
    // Set the result directly - the default handler will be skipped
    context.SetProperty(new OrchestratorExecutionResult
    {
        Actions = actions,
        CustomStatus = "Executed out-of-process"
    });
    
    // Don't call next() if you're providing the result yourself
});
```

## Next Steps

- [Entities](entities.md) — Durable Entities pattern
- [Serialization](serialization.md) — Custom data converters
- [Testing](testing.md) — Testing orchestrations
