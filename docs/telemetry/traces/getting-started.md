# Getting Started - Distributed Tracing

> ⚠ Important: durable task distributed tracing is currently [experimental](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/document-status.md). The schema is subject to changes until it is marked as stable. These changes may occur in any package update.

> ⚠ Important: this guide only applies DurableTask users. For Durable Functions, please see [here](https://github.com/Azure/azure-functions-durable-extension/blob/dev/samples/distributed-tracing/v2/DistributedTracingSample/README.md)

Distributed tracing in DurableTask uses the `ActivitySource` approach, it is both OpenTelemetry and Application Insights compatible.

## OpenTelemetry

Add the `"DurableTask.Core"` source to the OTel trace builder.

``` CSharp
Sdk.CreateTracerProviderBuilder()
    .AddSource("DurableTask.Core")
    .Build()
```

See [sample](../../../samples/DistributedTraceSample/OpenTelemetry)

## Application Insights

1. Add reference to [Microsoft.Azure.DurableTask.ApplicationInsights](https://www.nuget.org/packages/Microsoft.Azure.DurableTask.ApplicationInsights)
2. Add the `DurableTelemetryModule` to AppInsights: `services.TryAddEnumerable(ServiceDescriptor.Singleton<ITelemetryModule, DurableTelemetryModule>());`

See [sample](../../../samples/DistributedTraceSample/ApplicationInsights)
