# Application Insights Integration

The Durable Task Framework provides deep integration with Azure Application Insights for monitoring, diagnostics, and performance analysis.

## Installation

```bash
dotnet add package Microsoft.Azure.DurableTask.ApplicationInsights
dotnet add package Microsoft.ApplicationInsights
```

## Setup

### Basic Configuration

```csharp
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.DurableTask.ApplicationInsights;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();

// Add Application Insights
services.AddApplicationInsightsTelemetryWorkerService(options =>
{
    options.ConnectionString = "InstrumentationKey=your-key;...";
});

// Add DurableTask telemetry module for distributed tracing
services.TryAddEnumerable(
    ServiceDescriptor.Singleton<ITelemetryModule, DurableTelemetryModule>());

var serviceProvider = services.BuildServiceProvider();
```

### ASP.NET Core Integration

```csharp
// In Program.cs
var builder = WebApplication.CreateBuilder(args);

// Add Application Insights
builder.Services.AddApplicationInsightsTelemetry();

// Add DurableTask telemetry module
builder.Services.TryAddEnumerable(
    ServiceDescriptor.Singleton<ITelemetryModule, DurableTelemetryModule>());

var app = builder.Build();
```

### Console Application

```csharp
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.DurableTask.ApplicationInsights;
using DurableTask.Core;
using DurableTask.Emulator;

// Configure Application Insights
var configuration = TelemetryConfiguration.CreateDefault();
configuration.ConnectionString = "InstrumentationKey=...";

// Add the DurableTask telemetry module
var module = new DurableTelemetryModule();
module.Initialize(configuration);

var telemetryClient = new TelemetryClient(configuration);

// Create logger factory for diagnostics
using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddConsole();
    builder.SetMinimumLevel(LogLevel.Information);
});

// Create DTFx components
var service = new LocalOrchestrationService();
var worker = new TaskHubWorker(service, loggerFactory);
worker.AddTaskOrchestrations(typeof(MyOrchestration));
worker.AddTaskActivities(typeof(MyActivity));

await worker.StartAsync();

// ... run orchestrations ...

await worker.StopAsync();

// Ensure telemetry is flushed
telemetryClient.Flush();
await Task.Delay(TimeSpan.FromSeconds(5));
```

## What Gets Tracked

### Automatic Telemetry

The `DurableTelemetryModule` automatically tracks:

| Telemetry Type | Description |
|----------------|-------------|
| **Requests** | Orchestration and activity executions |
| **Dependencies** | Activity calls, sub-orchestrations |
| **Traces** | Log messages from DTFx |
| **Exceptions** | Failures in orchestrations and activities |
| **Custom Events** | Orchestration lifecycle events |

### Distributed Tracing

Operations are automatically correlated:

```text
Request: OrderOrchestration (parent)
├── Dependency: ValidateOrderActivity
├── Dependency: ProcessPaymentActivity
├── Dependency: ShippingOrchestration (sub-orchestration)
│   └── Dependency: CreateShipmentActivity
└── Dependency: SendConfirmationActivity
```

## Custom Telemetry

### Adding Custom Properties

```csharp
public class OrderOrchestration : TaskOrchestration<OrderResult, OrderInput>
{
    private readonly TelemetryClient _telemetryClient;
    
    public OrderOrchestration(TelemetryClient telemetryClient)
    {
        _telemetryClient = telemetryClient;
    }
    
    public override async Task<OrderResult> RunTask(
        OrchestrationContext context, 
        OrderInput input)
    {
        // Track custom event
        if (!context.IsReplaying)
        {
            _telemetryClient.TrackEvent("OrderProcessingStarted", new Dictionary<string, string>
            {
                ["InstanceId"] = context.OrchestrationInstance.InstanceId,
                ["OrderId"] = input.OrderId,
                ["CustomerId"] = input.CustomerId
            });
        }
        
        // ... orchestration logic ...
        
        if (!context.IsReplaying)
        {
            _telemetryClient.TrackEvent("OrderProcessingCompleted", new Dictionary<string, string>
            {
                ["InstanceId"] = context.OrchestrationInstance.InstanceId,
                ["OrderId"] = input.OrderId,
                ["Status"] = "Success"
            });
        }
        
        return result;
    }
}
```

### Tracking Metrics

```csharp
if (!context.IsReplaying)
{
    _telemetryClient.TrackMetric("OrderProcessingDuration", 
        (context.CurrentUtcDateTime - startTime).TotalMilliseconds);
    
    _telemetryClient.TrackMetric("OrderItemCount", input.Items.Count);
}
```

### Tracking Exceptions

```csharp
try
{
    await context.ScheduleTask<string>(typeof(RiskyActivity), input);
}
catch (TaskFailedException ex)
{
    if (!context.IsReplaying)
    {
        _telemetryClient.TrackException(ex.InnerException, new Dictionary<string, string>
        {
            ["InstanceId"] = context.OrchestrationInstance.InstanceId,
            ["ActivityName"] = ex.Name
        });
    }
    throw;
}
```

## Querying Data

### Kusto Queries (Log Analytics)

**Orchestration execution times:**
```kusto
requests
| where name contains "orchestration"
| summarize avg(duration), percentile(duration, 95) by name
| order by avg_duration desc
```

**Failed orchestrations:**
```kusto
requests
| where name contains "orchestration" and success == false
| project timestamp, name, duration, customDimensions
| order by timestamp desc
```

**Activity performance:**
```kusto
dependencies
| where type == "DurableTask"
| summarize count(), avg(duration) by name
| order by count_ desc
```

**End-to-end traces:**
```kusto
union requests, dependencies, traces
| where operation_Id == "your-operation-id"
| order by timestamp asc
| project timestamp, itemType, name, message, duration
```

## Live Metrics

Application Insights Live Metrics shows real-time:

- Incoming request rate
- Failure rate
- Dependency call duration
- Server response time

Enable Live Metrics in your configuration:

```csharp
services.AddApplicationInsightsTelemetryWorkerService(options =>
{
    options.ConnectionString = "...";
    options.EnableLiveMetrics = true;
});
```

## Alerts

Configure alerts for common scenarios:

### High Failure Rate

```kusto
requests
| where name contains "orchestration"
| summarize failureCount = countif(success == false), totalCount = count() by bin(timestamp, 5m)
| extend failureRate = failureCount * 100.0 / totalCount
| where failureRate > 5
```

### Long-Running Orchestrations

```kusto
requests
| where name contains "orchestration"
| where duration > 300000  // 5 minutes
| project timestamp, name, duration, operation_Id
```

### Stuck Orchestrations

Monitor for orchestrations that haven't progressed:

```kusto
customEvents
| where name == "OrchestrationStarted"
| join kind=leftanti (
    customEvents
    | where name == "OrchestrationCompleted"
    | project completedInstanceId = tostring(customDimensions["InstanceId"])
) on $left.customDimensions["InstanceId"] == $right.completedInstanceId
| where timestamp < ago(1h)
```

## Sampling

For high-volume scenarios, configure sampling:

```csharp
services.AddApplicationInsightsTelemetryWorkerService(options =>
{
    options.ConnectionString = "...";
});

services.Configure<TelemetryConfiguration>(config =>
{
    config.DefaultTelemetrySink.TelemetryProcessorChainBuilder
        .UseAdaptiveSampling(maxTelemetryItemsPerSecond: 5)
        .Build();
});
```

## Best Practices

### 1. Use IsReplaying for Custom Telemetry

```csharp
if (!context.IsReplaying)
{
    _telemetryClient.TrackEvent("CustomEvent");
}
```

### 2. Include Correlation IDs

```csharp
_telemetryClient.TrackEvent("OrderProcessed", new Dictionary<string, string>
{
    ["InstanceId"] = context.OrchestrationInstance.InstanceId,
    ["OrderId"] = input.OrderId
});
```

### 3. Flush Before Shutdown

```csharp
await worker.StopAsync();
telemetryClient.Flush();
await Task.Delay(TimeSpan.FromSeconds(5));  // Allow time for flush
```

### 4. Monitor Key Metrics

- Orchestration success/failure rate
- Activity duration
- Queue depth (if applicable)
- Concurrent orchestrations

## Samples

See the complete working sample:
- [Application Insights Sample](../../samples/DistributedTraceSample/ApplicationInsights)

## Next Steps

- [Distributed Tracing](distributed-tracing.md) — OpenTelemetry integration
- [Logging](logging.md) — Structured logging
- [Support](../support.md) — Getting help
