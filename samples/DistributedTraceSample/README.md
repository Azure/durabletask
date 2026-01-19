# Distributed Trace Samples

This directory contains samples demonstrating telemetry integration with different distributed tracing providers for Durable Task applications.

## Overview

Distributed tracing allows you to monitor and debug orchestrations across your entire application stack. These samples show how to configure various telemetry exporters.

## Samples

| Sample | Description |
| ------ | ----------- |
| [OpenTelemetry](OpenTelemetry/) | Integration with OpenTelemetry for vendor-neutral distributed tracing |
| [ApplicationInsights](ApplicationInsights/) | Integration with Azure Application Insights |

## OpenTelemetry Sample

The [OpenTelemetry sample](OpenTelemetry/) demonstrates how to configure distributed tracing with multiple exporters including Console, Application Insights, and Zipkin.

```csharp
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("MySample"))
    .AddSource("DurableTask.Core")
    .AddConsoleExporter()
    .AddZipkinExporter()
    .AddAzureMonitorTraceExporter(options =>
    {
        options.ConnectionString = Environment.GetEnvironmentVariable("AZURE_MONITOR_CONNECTION_STRING");
    })
    .Build();
```

See the [OpenTelemetry README](OpenTelemetry/README.md) for detailed setup instructions.

## Application Insights Sample

The [Application Insights sample](ApplicationInsights/) demonstrates direct integration with Azure Application Insights without OpenTelemetry.

```csharp
services.AddApplicationInsightsTelemetryWorkerService();
services.TryAddEnumerable(
    ServiceDescriptor.Singleton<ITelemetryModule, DurableTelemetryModule>());
```

## Prerequisites

- .NET 6.0 SDK or later
- Azure Storage Emulator (Azurite) or Azure Storage account
- (Optional) Application Insights resource
- (Optional) Zipkin instance for OpenTelemetry sample

## Additional Resources

- [Distributed Tracing Guide](../../docs/telemetry/distributed-tracing.md)
- [Application Insights Documentation](../../docs/telemetry/application-insights.md)
- [OpenTelemetry Documentation](https://opentelemetry.io/)
