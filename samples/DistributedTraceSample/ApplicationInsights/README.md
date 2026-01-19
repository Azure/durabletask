# Application Insights Sample

This sample demonstrates direct integration with Azure Application Insights for distributed tracing in Durable Task applications.

## Prerequisites

- .NET 6.0 SDK or later
- Azure Storage Emulator (Azurite) or Azure Storage account
- Azure Application Insights resource

## Configuration

1. Create an Application Insights resource in the Azure Portal

2. Configure the connection string in `appsettings.json`:

   ```json
   {
     "ApplicationInsights": {
       "ConnectionString": "InstrumentationKey=..."
     }
   }
   ```

   Or set the environment variable:

   ```text
   APPLICATIONINSIGHTS_CONNECTION_STRING=InstrumentationKey=...
   ```

## Code Setup

```csharp
services.AddApplicationInsightsTelemetryWorkerService();
services.TryAddEnumerable(
    ServiceDescriptor.Singleton<ITelemetryModule, DurableTelemetryModule>());
```

The `FilterOutStorageTelemetryProcessor` is included to reduce noise from Azure Storage operations in your telemetry.

## Running the Sample

```bash
dotnet run
```

## Viewing Traces

1. Navigate to your Application Insights resource in the Azure Portal
2. Go to **Transaction Search**
3. Click on an entry to view the end-to-end transaction
4. A Gantt chart will show the visual representation of the trace and spans

## Additional Resources

- [Application Insights Documentation](../../../docs/telemetry/application-insights.md)
- [Distributed Tracing Guide](../../../docs/telemetry/distributed-tracing.md)
