# Distributed Tracing

The Durable Task Framework supports distributed tracing using the standard .NET `ActivitySource` API, compatible with both OpenTelemetry and Application Insights.

## Overview

Distributed tracing provides visibility into orchestration execution across services and activities. DTFx emits spans for:

- Starting orchestrations
- Running orchestrations
- Starting and running activities
- Sub-orchestrations
- Timers
- External events

## Supported Protocols

DTFx supports trace context propagation using standard protocols:

| Protocol | Description |
| -------- | ----------- |
| **W3C TraceContext** | W3C standard for distributed tracing (default) |
| **HTTP Correlation Protocol** | Legacy Application Insights protocol |

## OpenTelemetry Setup

### Installation

```bash
dotnet add package OpenTelemetry
dotnet add package OpenTelemetry.Exporter.Console  # Or your preferred exporter
```

### Configuration

Add the `DurableTask.Core` source to the OpenTelemetry trace builder:

```csharp
using OpenTelemetry;
using OpenTelemetry.Trace;

var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddSource("DurableTask.Core")
    .AddConsoleExporter()  // Or your preferred exporter
    .Build();
```

### Full Example

```csharp
using OpenTelemetry;
using OpenTelemetry.Trace;
using DurableTask.Core;
using DurableTask.AzureStorage;

// Configure OpenTelemetry
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddSource("DurableTask.Core")
    .AddConsoleExporter()
    .Build();

// Create logger factory
using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddConsole();
    builder.SetMinimumLevel(LogLevel.Information);
});

// Set up DTFx
var settings = new AzureStorageOrchestrationServiceSettings
{
    TaskHubName = "MyTaskHub",
    StorageAccountClientProvider = new StorageAccountClientProvider(connectionString),
    LoggerFactory = loggerFactory
};

var service = new AzureStorageOrchestrationService(settings);
await service.CreateIfNotExistsAsync();

var worker = new TaskHubWorker(service, loggerFactory);
worker.AddTaskOrchestrations(typeof(MyOrchestration));
worker.AddTaskActivities(typeof(MyActivity));

await worker.StartAsync();

var client = new TaskHubClient(service, loggerFactory: loggerFactory);
var instance = await client.CreateOrchestrationInstanceAsync(
    typeof(MyOrchestration),
    "input");

await client.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(1));
await worker.StopAsync();
```

### Exporting to Azure Monitor

```csharp
using Azure.Monitor.OpenTelemetry.Exporter;

var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddSource("DurableTask.Core")
    .AddAzureMonitorTraceExporter(o =>
    {
        o.ConnectionString = "InstrumentationKey=...";
    })
    .Build();
```

### Exporting to Jaeger

```csharp
using OpenTelemetry.Exporter;

var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddSource("DurableTask.Core")
    .AddJaegerExporter(o =>
    {
        o.AgentHost = "localhost";
        o.AgentPort = 6831;
    })
    .Build();
```

## Application Insights Setup

For Application Insights integration, use the dedicated telemetry module.

### Application Insights Installation

```bash
dotnet add package Microsoft.Azure.DurableTask.ApplicationInsights
dotnet add package Microsoft.ApplicationInsights
```

### Application Insights Configuration

```csharp
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Azure.DurableTask.ApplicationInsights;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();

// Add Application Insights
services.AddApplicationInsightsTelemetryWorkerService(options =>
{
    options.ConnectionString = "InstrumentationKey=...";
});

// Add DurableTask telemetry module
services.TryAddEnumerable(
    ServiceDescriptor.Singleton<ITelemetryModule, DurableTelemetryModule>());

var serviceProvider = services.BuildServiceProvider();
```

### ASP.NET Core Integration

```csharp
// In Program.cs or Startup.cs
builder.Services.AddApplicationInsightsTelemetry();
builder.Services.TryAddEnumerable(
    ServiceDescriptor.Singleton<ITelemetryModule, DurableTelemetryModule>());
```

## Span Reference

### Orchestration Spans

| Span Name | Kind | Description |
| --------- | ---- | ----------- |
| `create_orchestration:{name}` | Producer | Starting an orchestration from client |
| `orchestration:{name}` | Server | Running an orchestration in worker |
| `orchestration:{name}` | Client | Starting a sub-orchestration |

### Activity Spans

| Span Name | Kind | Description |
| --------- | ---- | ----------- |
| `activity:{name}` | Client | Starting an activity from orchestration |
| `activity:{name}` | Server | Running an activity in worker |

### Other Spans

| Span Name | Kind | Description |
| --------- | ---- | ----------- |
| `timer` | Internal | Durable timer |
| `event:{name}` | Producer | Sending an external event |

## Attributes

DTFx spans include these attributes:

| Attribute | Type | Description |
| --------- | ---- | ----------- |
| `durabletask.type` | string | Type: "orchestration", "activity", "timer", "event" |
| `durabletask.task.name` | string | Name of the task |
| `durabletask.task.version` | string | Version of the task (if specified) |
| `durabletask.task.instance_id` | string | Orchestration instance ID |
| `durabletask.task.execution_id` | string | Execution ID |
| `durabletask.task.task_id` | int | Task index within orchestration |
| `durabletask.task.result` | string | Result: "Succeeded", "Failed", "Terminated" |

## Trace Correlation

Traces are automatically correlated across:

- Parent orchestration → Sub-orchestration
- Orchestration → Activity
- Client → Orchestration

### Example Trace Hierarchy

```text
create_orchestration:OrderOrchestration (Producer)
└── orchestration:OrderOrchestration (Server)
    ├── activity:ValidateOrder (Client)
    │   └── activity:ValidateOrder (Server)
    ├── activity:ProcessPayment (Client)
    │   └── activity:ProcessPayment (Server)
    └── orchestration:ShippingOrchestration (Client)
        └── orchestration:ShippingOrchestration (Server)
            └── activity:CreateShipment (Client)
                └── activity:CreateShipment (Server)
```

## Samples

See the sample projects for complete working examples:

- [OpenTelemetry Sample](../../samples/DistributedTraceSample/OpenTelemetry) — Modern ActivitySource-based tracing with OpenTelemetry
- [Application Insights Sample](../../samples/DistributedTraceSample/ApplicationInsights) — Modern ActivitySource-based tracing with Application Insights
- [Correlation Sample](../../samples/Correlation.Samples) — Legacy CorrelationSettings-based tracing (Azure Storage only)

## Legacy Correlation (Azure Storage Only)

The Azure Storage provider includes a legacy correlation system using `CorrelationSettings`. This approach predates the modern `ActivitySource` API and is maintained for backward compatibility.

### Enabling Legacy Correlation

```csharp
using DurableTask.Core.Settings;

// Enable legacy distributed tracing
CorrelationSettings.Current.EnableDistributedTracing = true;
CorrelationSettings.Current.Protocol = Protocol.W3CTraceContext; // or Protocol.HttpCorrelationProtocol
```

### Setting Up Telemetry

The legacy system requires manual setup of `CorrelationTraceClient`:

```csharp
using DurableTask.Core;
using Microsoft.ApplicationInsights;

// Set up telemetry callbacks
CorrelationTraceClient.SetUp(
    (TraceContextBase requestTraceContext) =>
    {
        requestTraceContext.Stop();
        var requestTelemetry = requestTraceContext.CreateRequestTelemetry();
        telemetryClient.TrackRequest(requestTelemetry);
    },
    (TraceContextBase dependencyTraceContext) =>
    {
        dependencyTraceContext.Stop();
        var dependencyTelemetry = dependencyTraceContext.CreateDependencyTelemetry();
        telemetryClient.TrackDependency(dependencyTelemetry);
    },
    (Exception e) =>
    {
        telemetryClient.TrackException(e);
    }
);
```

> [!NOTE]
> The modern `ActivitySource` approach (OpenTelemetry/DurableTelemetryModule) is recommended for new projects. The legacy `CorrelationSettings` system only works with the Azure Storage provider.

## Next Steps

- [Logging](logging.md) — Structured logging in DTFx
- [Application Insights](application-insights.md) — Full AI integration
- [Semantic Conventions](traces/semantic-conventions.md) — Detailed span specification
