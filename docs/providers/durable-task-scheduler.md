# Durable Task Scheduler

The **Durable Task Scheduler** is a fully managed Azure service purpose-built for running durable orchestrations. It provides the best experience for production workloads with zero infrastructure management and built-in enterprise support.

> ⭐ **Recommended**: For new projects, we recommend the Durable Task Scheduler as your backend provider.

## Overview

| Feature | Benefit |
| ------- | ------- |
| **Fully Managed** | No storage accounts, databases, or infrastructure to manage |
| **Built-in Dashboard** | Monitor orchestrations without additional tooling |
| **Highest Throughput** | Purpose-built for durable workflow performance |
| **Azure Support** | 24/7 enterprise support with SLA (with Azure support plan) |
| **Managed Identity** | Secure authentication using Azure AD |
| **Local Emulator** | Docker-based emulator for development |

For complete documentation on creating and configuring Azure resources, authentication, SKUs, RBAC, and pricing, see the [official Azure documentation](https://learn.microsoft.com/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler).

## Installation

```bash
dotnet add package Microsoft.DurableTask.AzureManagedBackend
```

## DTFx Code Sample

```csharp
using DurableTask.Core;
using Microsoft.DurableTask.AzureManagedBackend;
using Microsoft.Extensions.Logging;

// Get connection string from environment
// Expected format: "Endpoint=https://<host>;Authentication=<credentialType>;TaskHub=<hubName>"
string? connectionString = Environment.GetEnvironmentVariable("DTS_CONNECTION_STRING");
if (string.IsNullOrWhiteSpace(connectionString))
{
    Console.Error.WriteLine("An environment variable named DTS_CONNECTION_STRING is required.");
    return;
}

// Configure logging
ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
    builder.AddSimpleConsole(options =>
    {
        options.SingleLine = true;
        options.UseUtcTimestamp = true;
        options.TimestampFormat = "yyyy-MM-ddTHH:mm:ss.fffZ ";
    }));

// Create the orchestration service for Durable Task Scheduler
AzureManagedOrchestrationService service = new(
    AzureManagedOrchestrationServiceOptions.FromConnectionString(connectionString),
    loggerFactory);

// Create and configure the worker
TaskHubWorker worker = new(service, loggerFactory);
worker.AddTaskOrchestrations(typeof(HelloWorldOrchestration));
worker.AddTaskActivities(typeof(HelloActivity));

// Start the worker
await worker.StartAsync();

// Create a client and start an orchestration
TaskHubClient client = new(service, null, loggerFactory);
OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(
    orchestrationType: typeof(HelloWorldOrchestration),
    input: null);

Console.WriteLine($"Started orchestration with ID = '{instance.InstanceId}'");

// Wait for completion
OrchestrationState state = await client.WaitForOrchestrationAsync(
    instance, 
    TimeSpan.FromMinutes(1));

Console.WriteLine($"Orchestration completed with status: {state.OrchestrationStatus}");
Console.WriteLine($"Output: {state.Output}");

// Clean up
await worker.StopAsync();
service.Dispose();
```

## Connection String Format

```text
Endpoint=<scheduler-url>;TaskHub=<taskhub-name>;Authentication=<auth-type>
```

See [Authentication Types](#authentication-types) for supported credential types.

## Configuration Options

The `AzureManagedOrchestrationServiceOptions` class provides configuration for the Durable Task Scheduler backend.

### Creating Options

**From connection string (recommended):**

```csharp
var options = AzureManagedOrchestrationServiceOptions.FromConnectionString(connectionString);
```

**Manual construction:**

```csharp
var options = new AzureManagedOrchestrationServiceOptions(
    address: "https://myscheduler.westus3.durabletask.io",
    credential: new DefaultAzureCredential());
options.TaskHubName = "my-task-hub";
```

### Authentication Types

The connection string `Authentication` property supports these credential types:

| Value | Credential Type | Use Case |
| ----- | --------------- | -------- |
| `DefaultAzure` | `DefaultAzureCredential` | General purpose; tries multiple auth methods |
| `ManagedIdentity` | `ManagedIdentityCredential` | Azure-hosted apps; add `ClientId` for user-assigned |
| `WorkloadIdentity` | `WorkloadIdentityCredential` | Kubernetes, CI/CD pipelines, SPIFFE |
| `Environment` | `EnvironmentCredential` | Container apps with env var credentials |
| `AzureCLI` | `AzureCliCredential` | Local dev with `az login` |
| `AzurePowerShell` | `AzurePowerShellCredential` | Local dev with `Connect-AzAccount` |
| `VisualStudio` | `VisualStudioCredential` | Local dev from Visual Studio |
| `InteractiveBrowser` | `InteractiveBrowserCredential` | Interactive scenarios (not for production) |
| `None` | No authentication | Local emulator only |

**User-assigned managed identity example:**

```text
Endpoint=https://myscheduler.westus3.durabletask.io;TaskHub=default;Authentication=ManagedIdentity;ClientId=00000000-0000-0000-0000-000000000000
```

### Concurrency Settings

Control how many work items are processed in parallel:

| Property | Default | Description |
| -------- | ------- | ----------- |
| `MaxConcurrentOrchestrationWorkItems` | `ProcessorCount * 10` | Max parallel orchestration executions |
| `MaxConcurrentActivityWorkItems` | `ProcessorCount * 10` | Max parallel activity executions |

```csharp
var options = AzureManagedOrchestrationServiceOptions.FromConnectionString(connectionString);
options.MaxConcurrentOrchestrationWorkItems = 50;
options.MaxConcurrentActivityWorkItems = 100;
```

> [!TIP]
> Increase activity concurrency for I/O-bound workloads. Reduce orchestration concurrency if orchestrations consume significant memory.

### Large Payload Storage

For payloads exceeding gRPC message limits, configure Azure Blob Storage to externalize large data:

```csharp
var options = AzureManagedOrchestrationServiceOptions.FromConnectionString(connectionString);
options.LargePayloadStorageOptions = new LargePayloadStorageOptions("UseDevelopmentStorage=true")
{
    ExternalizeThresholdBytes = 1024,      // Externalize payloads larger than 1KB
    MaxExternalizedPayloadBytes = 4194304, // Max 4MB payload size
    CompressPayloads = true                // Compress before storing (default: true)
};
```

| Property | Description |
| -------- | ----------- |
| Constructor (`connectionString`) | Azure Storage connection string or `"UseDevelopmentStorage=true"` for local dev |
| `ExternalizeThresholdBytes` | Payloads larger than this are stored in blob storage |
| `MaxExternalizedPayloadBytes` | Maximum allowed payload size (fails fast if exceeded) |
| `CompressPayloads` | Whether to compress payloads before storing (improves storage efficiency) |

When enabled, large orchestration inputs, outputs, activity results, and event payloads are automatically stored in blob storage and retrieved transparently.

### Additional Options

| Property | Default | Description |
| -------- | ------- | ----------- |
| `TaskHubName` | `"default"` | Name of the task hub (usually set via connection string) |
| `OrchestrationHistoryCacheExpirationPeriod` | 10 minutes | How long orchestration history is cached in memory |
| `ResourceId` | `https://durabletask.io` | OAuth resource ID (change only for sovereign clouds) |

## Local Development with Emulator

For local development, use the Docker-based emulator:

```bash
docker pull mcr.microsoft.com/dts/dts-emulator:latest
docker run -d -p 8080:8080 -p 8082:8082 mcr.microsoft.com/dts/dts-emulator:latest
```

Connect to the emulator:

```csharp
var connectionString = "Endpoint=http://localhost:8080;TaskHub=default;Authentication=None";
var service = new AzureManagedOrchestrationService(
    AzureManagedOrchestrationServiceOptions.FromConnectionString(connectionString),
    loggerFactory);
```

Access the local dashboard at: `http://localhost:8082`

## Samples

For complete working examples, see the [Durable Task Scheduler samples repository](https://github.com/Azure-Samples/Durable-Task-Scheduler/tree/main/samples/dtfx).

## Additional Resources

- [Azure Documentation](https://learn.microsoft.com/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler) — Creating resources, configuration, SKUs, RBAC, pricing
- [Quickstart Guide](https://learn.microsoft.com/azure/azure-functions/durable/durable-task-scheduler/quickstart-durable-task-scheduler)
- [Azure Samples Repository](https://github.com/Azure-Samples/Durable-Task-Scheduler/)
- [Support](../support.md) — Enterprise support options

## Next Steps

- [Choosing a Backend](../getting-started/choosing-a-backend.md) — Compare all providers
- [Quickstart](../getting-started/quickstart.md) — Create your first orchestration
- [Core Concepts](../concepts/core-concepts.md) — Learn the fundamentals
