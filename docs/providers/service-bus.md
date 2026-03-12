# Service Bus Provider

The Service Bus provider uses Azure Service Bus for orchestration messaging. It's suitable for scenarios requiring Service Bus integration or existing Service Bus infrastructure.

> [!WARNING]
> The Service Bus provider is in maintenance mode and is not recommended for new projects. Consider using [Durable Task Scheduler](durable-task-scheduler.md) for a managed alternative or the [Azure Storage Provider](azure-storage.md) for self-managed deployments.

## Installation

```bash
dotnet add package Microsoft.Azure.DurableTask.ServiceBus
```

## Configuration

### Basic Setup

The Service Bus provider requires separate stores for messaging (Service Bus) and history/state (Azure Storage):

```csharp
using DurableTask.ServiceBus;
using DurableTask.ServiceBus.Settings;
using DurableTask.ServiceBus.Tracking;
using DurableTask.Core;
using Microsoft.Extensions.Logging;

string serviceBusConnectionString = "Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...";
string storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...";
string taskHubName = "MyTaskHub";

using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddConsole();
    builder.SetMinimumLevel(LogLevel.Information);
});

// Create the instance store (Azure Table Storage for history)
var instanceStore = new AzureTableInstanceStore(taskHubName, storageConnectionString);

// Create the blob store (Azure Blob Storage for large messages/sessions)
var blobStore = new AzureStorageBlobStore(taskHubName, storageConnectionString);

// Configure settings
var settings = new ServiceBusOrchestrationServiceSettings();

// Create the orchestration service
var service = new ServiceBusOrchestrationService(
    serviceBusConnectionString,
    taskHubName,
    instanceStore,
    blobStore,
    settings);

await service.CreateIfNotExistsAsync();

var worker = new TaskHubWorker(service, loggerFactory);
var client = new TaskHubClient(service, loggerFactory: loggerFactory);
```

### Using Managed Identity

The Service Bus provider supports managed identity authentication (.NET Standard 2.0+):

```csharp
using Azure.Identity;
using DurableTask.ServiceBus;
using DurableTask.ServiceBus.Settings;
using DurableTask.ServiceBus.Tracking;

string serviceBusNamespace = "mynamespace.servicebus.windows.net";
Uri storageEndpoint = new Uri("https://mystorageaccount.table.core.windows.net");
Uri blobEndpoint = new Uri("https://mystorageaccount.blob.core.windows.net");
string taskHubName = "MyTaskHub";

var credential = new DefaultAzureCredential();

// Create stores with managed identity
var instanceStore = new AzureTableInstanceStore(taskHubName, storageEndpoint, credential);
var blobStore = new AzureStorageBlobStore(taskHubName, blobEndpoint, credential);

var settings = new ServiceBusOrchestrationServiceSettings();

// Create Service Bus connection with managed identity
var service = new ServiceBusOrchestrationService(
    serviceBusNamespace,  // Just the hostname, not a connection string
    credential,
    taskHubName,
    instanceStore,
    blobStore,
    settings);
```

## Configuration Options

### ServiceBusOrchestrationServiceSettings

| Setting | Description | Default |
| ------- | ----------- | ------- |
| `MaxTaskOrchestrationDeliveryCount` | Max delivery attempts for orchestration messages | 10 |
| `MaxTaskActivityDeliveryCount` | Max delivery attempts for activity messages | 10 |
| `MaxTrackingDeliveryCount` | Max delivery attempts for tracking messages | 10 |
| `MaxQueueSizeInMegabytes` | Maximum queue size for Service Bus queues | 1024 |
| `PrefetchCount` | Message prefetch count | 50 |
| `TaskOrchestrationDispatcherSettings` | Orchestration dispatcher configuration | See below |
| `TaskActivityDispatcherSettings` | Activity dispatcher configuration | See below |
| `MessageCompressionSettings` | Message compression configuration | Disabled |
| `JumpStartSettings` | Jump start (stale instance recovery) settings | Enabled |

### Dispatcher Settings

Dispatcher settings control concurrency:

```csharp
var settings = new ServiceBusOrchestrationServiceSettings
{
    TaskOrchestrationDispatcherSettings =
    {
        MaxConcurrentOrchestrations = 100,
        CompressOrchestrationState = true
    },
    TaskActivityDispatcherSettings =
    {
        MaxConcurrentActivities = 100
    }
};
```

## Architecture

### Service Bus Resources

The provider creates these Service Bus entities:

| Entity Type | Name Pattern | Purpose |
| ----------- | ------------ | ------- |
| **Orchestrator Queue** | `{taskhub}/orchestrator` | Orchestration messages |
| **Worker Queue** | `{taskhub}/worker` | Activity messages |
| **Tracking Queue** | `{taskhub}/tracking` | Tracking events |

### Storage Resources

In addition to Service Bus, the provider uses Azure Storage:

| Resource | Name Pattern | Purpose |
| -------- | ------------ | ------- |
| **Instance History Table** | `InstanceHistory00{taskhub}` | Orchestration state and execution history |
| **Jump Start Table** | `JumpStart00{taskhub}` | Pending orchestrations for stale instance recovery |
| **Blob Container** | `{taskhub}-dtfx` | Large messages and session state |

> [!NOTE]
> Unlike the Azure Storage provider (which has separate History and Instances tables), the Service Bus provider stores both instance metadata and history events in a single `InstanceHistory` table.

## Limitations

- Requires both Service Bus and Azure Storage
- Limited query capabilities compared to Azure Storage provider
- Less commonly used — smaller community and fewer examples
- No built-in monitoring dashboard

## Next Steps

- [Choosing a Backend](../getting-started/choosing-a-backend.md) — Compare all providers
- [Durable Task Scheduler](durable-task-scheduler.md) — Recommended managed alternative
- [Azure Storage Provider](azure-storage.md) — Alternative self-managed option
