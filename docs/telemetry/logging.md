# Logging

The Durable Task Framework provides structured logging for observability and debugging. This guide covers logging configuration and best practices.

## Log Sources

DTFx emits logs from these sources:

| Source | Description |
| ------ | ----------- |
| `DurableTask.Core` | Core framework operations |
| `DurableTask.AzureStorage` | Azure Storage provider |
| `DurableTask.ServiceBus` | Service Bus provider |
| `DurableTask.AzureManagedBackend` | Durable Task Scheduler |

## Configuring Logging

### With Microsoft.Extensions.Logging

```csharp
using Microsoft.Extensions.Logging;

var loggerFactory = LoggerFactory.Create(builder =>
{
    builder
        .SetMinimumLevel(LogLevel.Information)
        .AddConsole()
        .AddFilter("DurableTask.Core", LogLevel.Debug);
});

// Configure provider-specific logging (e.g., Azure Storage)
var settings = new AzureStorageOrchestrationServiceSettings
{
    TaskHubName = "MyTaskHub",
    StorageAccountClientProvider = new StorageAccountClientProvider(
        "mystorageaccount", 
        new DefaultAzureCredential()),
    LoggerFactory = loggerFactory, // Provider logs
};
var service = new AzureStorageOrchestrationService(settings);

// Pass to worker and client
var worker = new TaskHubWorker(service, loggerFactory);
var client = new TaskHubClient(service, loggerFactory: loggerFactory);
```

> [!NOTE]
> Pass the `ILoggerFactory` to all three locations (provider settings, worker, and client) for complete log coverage. Provider-specific logs include message delivery times, partition operations, and other backend details useful for debugging.

### With Serilog

```csharp
using Serilog;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .MinimumLevel.Override("DurableTask.Core", Serilog.Events.LogEventLevel.Debug)
    .WriteTo.Console()
    .CreateLogger();

var loggerFactory = new LoggerFactory().AddSerilog();
var worker = new TaskHubWorker(service, loggerFactory);
```

### ASP.NET Core Integration

```csharp
// In Program.cs
builder.Logging.AddFilter("DurableTask.Core", LogLevel.Debug);
```

## Log Events

### Orchestration Events

| Event ID | Level | Description |
| -------- | ----- | ----------- |
| 40 | Information | Scheduling orchestration |
| 43 | Information | Waiting for orchestration |
| 49 | Information | Orchestration completed |
| 51 | Information | Executing orchestration logic |
| 52 | Information | Orchestration executed (scheduled operations) |

### Activity Events

| Event ID | Level | Description |
| -------- | ----- | ----------- |
| 46 | Information | Scheduling activity |
| 60 | Information | Starting activity |
| 61 | Information | Activity completed |

### Worker Events

| Event ID | Level | Description |
| -------- | ----- | ----------- |
| 10 | Information | Worker starting |
| 11 | Information | Worker started |
| 12 | Information | Worker stopping |
| 13 | Information | Worker stopped |

### Example Log Output

```text
info: DurableTask.Core[10] Durable task hub worker is starting
info: DurableTask.Core[40] Scheduling orchestration 'MyOrchestration' with instance ID = 'abc123'
info: DurableTask.Core[51] abc123: Executing 'MyOrchestration' orchestration logic
info: DurableTask.Core[46] abc123: Scheduling activity [MyActivity#0]
info: DurableTask.Core[60] abc123: Starting task activity [MyActivity#0]
info: DurableTask.Core[61] abc123: Task activity [MyActivity#0] completed successfully
info: DurableTask.Core[49] abc123: Orchestration completed with status 'Completed'
```

## Logging in Orchestrations

### Using IsReplaying

Avoid duplicate logs during replay. Note that DTFx orchestrations do not support constructor-based dependency injection. Use a static logger or pass a logger factory through your object creator:

```csharp
public class MyOrchestration : TaskOrchestration<string, string>
{
    // Use a static logger or configure via ObjectCreator
    private static readonly ILogger Logger = LoggerFactory
        .Create(builder => builder.AddConsole())
        .CreateLogger<MyOrchestration>();
    
    public override async Task<string> RunTask(
        OrchestrationContext context, 
        string input)
    {
        // Only log during actual execution, not replay
        if (!context.IsReplaying)
        {
            Logger.LogInformation(
                "Processing orchestration {InstanceId} with input {Input}",
                context.OrchestrationInstance.InstanceId,
                input);
        }
        
        var result = await context.ScheduleTask<string>(typeof(MyActivity), input);
        
        if (!context.IsReplaying)
        {
            Logger.LogInformation(
                "Orchestration {InstanceId} completed with result {Result}",
                context.OrchestrationInstance.InstanceId,
                result);
        }
        
        return result;
    }
}
```

### Structured Logging Best Practices

Include relevant context in log messages:

```csharp
// ✅ Good - structured with context
_logger.LogInformation(
    "Processing order {OrderId} for customer {CustomerId} in orchestration {InstanceId}",
    input.OrderId,
    input.CustomerId,
    context.OrchestrationInstance.InstanceId);

// ❌ Bad - string concatenation, no structure
_logger.LogInformation(
    "Processing order " + input.OrderId + " for customer " + input.CustomerId);
```

## Logging in Activities

Activities don't have replay concerns, so log freely. Like orchestrations, DTFx activities do not support constructor-based dependency injection. Use a static logger or configure via a custom `ObjectCreator`:

```csharp
public class MyActivity : AsyncTaskActivity<string, string>
{
    // Use a static logger or configure via ObjectCreator
    private static readonly ILogger Logger = LoggerFactory
        .Create(builder => builder.AddConsole())
        .CreateLogger<MyActivity>();
    
    protected override async Task<string> ExecuteAsync(
        TaskContext context, 
        string input)
    {
        Logger.LogInformation(
            "Starting activity for orchestration {InstanceId}",
            context.OrchestrationInstance.InstanceId);
        
        try
        {
            var result = await DoWorkAsync(input);
            
            Logger.LogInformation(
                "Activity completed for orchestration {InstanceId}",
                context.OrchestrationInstance.InstanceId);
            
            return result;
        }
        catch (Exception ex)
        {
            Logger.LogError(ex,
                "Activity failed for orchestration {InstanceId}",
                context.OrchestrationInstance.InstanceId);
            throw;
        }
    }
    
    private Task<string> DoWorkAsync(string input) => Task.FromResult(input);
}
```

## Log Correlation

### Correlation IDs

Include correlation IDs for end-to-end tracing:

```csharp
public override async Task<string> RunTask(
    OrchestrationContext context, 
    OrderInput input)
{
    using (Logger.BeginScope(new Dictionary<string, object>
    {
        ["InstanceId"] = context.OrchestrationInstance.InstanceId,
        ["OrderId"] = input.OrderId,
        ["CorrelationId"] = input.CorrelationId
    }))
    {
        if (!context.IsReplaying)
        {
            Logger.LogInformation("Starting order processing");
        }
        
        // ... orchestration logic
    }
}
```

### Distributed Tracing Integration

For trace correlation, see [Distributed Tracing](distributed-tracing.md).

## Log Levels

Recommended log level configuration:

| Environment | DurableTask.Core | Provider |
| ----------- | ---------------- | -------- |
| Development | Debug | Debug |
| Testing | Debug | Information |
| Production | Information | Warning |

### Configuration Example

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "DurableTask.Core": "Information",
      "DurableTask.AzureStorage": "Warning"
    }
  }
}
```

## Filtering Noisy Logs

Some operations generate many logs. Filter as needed:

```csharp
builder.Logging
    .AddFilter("DurableTask.Core", LogLevel.Information)
    // Reduce noise from Azure Storage provider
    .AddFilter("DurableTask.AzureStorage", LogLevel.Warning);
```

## Diagnostic Logging

For troubleshooting, enable debug logging:

```csharp
builder.Logging
    .SetMinimumLevel(LogLevel.Debug)
    .AddFilter("DurableTask", LogLevel.Debug);
```

This reveals:

- Message processing details
- Partition lease operations
- History loading/saving
- Timer scheduling

## Logging with Middleware

For cross-cutting logging concerns without modifying orchestration or activity code, use [middleware](../advanced/middleware.md). This approach lets you intercept all executions in one place:

```csharp
var worker = new TaskHubWorker(orchestrationService, loggerFactory);

// Add orchestration logging middleware
worker.AddOrchestrationDispatcherMiddleware(async (context, next) =>
{
    var instance = context.GetProperty<OrchestrationInstance>();
    var runtimeState = context.GetProperty<OrchestrationRuntimeState>();
    
    logger.LogInformation("Orchestration {Name} ({InstanceId}) starting", 
        runtimeState?.Name, instance?.InstanceId);
    
    var stopwatch = Stopwatch.StartNew();
    try
    {
        await next();
        logger.LogInformation("Orchestration {Name} ({InstanceId}) completed in {ElapsedMs}ms", 
            runtimeState?.Name, instance?.InstanceId, stopwatch.ElapsedMilliseconds);
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Orchestration {Name} ({InstanceId}) failed", 
            runtimeState?.Name, instance?.InstanceId);
        throw;
    }
});

// Add activity logging middleware
worker.AddActivityDispatcherMiddleware(async (context, next) =>
{
    var scheduledEvent = context.GetProperty<TaskScheduledEvent>();
    var instance = context.GetProperty<OrchestrationInstance>();
    
    logger.LogInformation("Activity {ActivityName} starting for {InstanceId}", 
        scheduledEvent?.Name, instance?.InstanceId);
    
    await next();
});
```

See [Middleware](../advanced/middleware.md) for complete examples.

## Event Source Logging

In addition to `ILogger`, DTFx also emits logs via [Event Source](https://docs.microsoft.com/dotnet/api/system.diagnostics.tracing.eventsource), which is used by platforms like Azure Functions and Azure App Service for automatic telemetry collection. Event Source logging is always enabled and captures additional correlation details.

For advanced Event Source configuration, including provider GUIDs and structured logging details, see the [source documentation](https://github.com/Azure/durabletask/blob/main/src/DurableTask.Core/Logging/README.md).

## Next Steps

- [Distributed Tracing](distributed-tracing.md) — OpenTelemetry integration
- [Application Insights](application-insights.md) — Full AI integration
- [Middleware](../advanced/middleware.md) — Cross-cutting concerns including logging
- [Error Handling](../features/error-handling.md) — Logging errors
