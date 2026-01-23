# Azure Storage Provider

The Azure Storage provider uses Azure Storage queues, tables, and blobs to persist orchestration state. It's a self-managed option suitable for existing Azure Storage deployments.

## When to Use Azure Storage

✅ **Good for:**

- Existing Azure Storage infrastructure
- Cost-sensitive workloads with low-to-moderate throughput
- Internal Azure services in Ring-1 or lower

⚠️ **Consider [Durable Task Scheduler](durable-task-scheduler.md) instead for:**

- New projects
- Enterprise support requirements
- High-throughput scenarios
- Zero infrastructure management
- Internal Azure services in Ring-2 or higher

## Installation

```bash
dotnet add package Microsoft.Azure.DurableTask.AzureStorage
```

## Configuration

### Basic Setup

```csharp
using DurableTask.AzureStorage;
using DurableTask.Core;
using Microsoft.Extensions.Logging;

using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddConsole();
    builder.SetMinimumLevel(LogLevel.Information);
});

var settings = new AzureStorageOrchestrationServiceSettings
{
    StorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;",
    TaskHubName = "MyTaskHub",
    LoggerFactory = loggerFactory
};

var service = new AzureStorageOrchestrationService(settings);
await service.CreateIfNotExistsAsync();

var worker = new TaskHubWorker(service, loggerFactory);
var client = new TaskHubClient(service, loggerFactory: loggerFactory);
```

### Using Managed Identity

```csharp
using Azure.Identity;
using DurableTask.AzureStorage;
using DurableTask.Core;
using Microsoft.Extensions.Logging;

using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddConsole();
    builder.SetMinimumLevel(LogLevel.Information);
});

// Uses DefaultAzureCredential
var credential = new DefaultAzureCredential();

var settings = new AzureStorageOrchestrationServiceSettings
{
    TaskHubName = "MyTaskHub",
    StorageAccountClientProvider = new StorageAccountClientProvider("mystorageaccount", credential),
    LoggerFactory = loggerFactory
};

var service = new AzureStorageOrchestrationService(settings);
```

> [!TIP]
> For complete runnable examples using managed identity, see the [Managed Identity Samples](../../samples/ManagedIdentitySample/).

## Configuration Options

### Core Settings

| Setting | Description | Default |
| ------- | ----------- | ------- |
| `TaskHubName` | Name of the task hub (alphanumeric, 3-45 chars) | Required |
| `StorageConnectionString` | Azure Storage connection string | Required* |
| `StorageAccountName` | Storage account name (for managed identity) | Required* |

*Either `StorageConnectionString` or `StorageAccountName` with credentials is required.

### Performance Settings

| Setting | Description | Default |
| ------- | ----------- | ------- |
| `PartitionCount` | Number of control queue partitions (1-16) | 4 |
| `ControlQueueBufferThreshold` | Max messages prefetched and buffered per partition | 64 |
| `MaxConcurrentTaskOrchestrationWorkItems` | Max concurrent orchestrations | 100 |
| `MaxConcurrentTaskActivityWorkItems` | Max concurrent activities | 10 |

### Partition Management

| Setting | Description | Default |
| ------- | ----------- | ------- |
| `UseTablePartitionManagement` | Use table-based partition management (recommended) | `true` |
| `UseLegacyPartitionManagement` | Use legacy blob-based partition management | `false` |
| `LeaseRenewInterval` | Interval for renewing partition leases | 10 seconds |
| `LeaseInterval` | Lease duration before expiration | 30 seconds |
| `LeaseAcquireInterval` | Interval for checking partition balance | 10 seconds |

> [!NOTE]
> Table-based partition management (`UseTablePartitionManagement = true`) is the default and recommended option. It provides better reliability for partition distribution and uses a `{taskhub}Partitions` table instead of blob leases. It's also significantly less expensive in terms of Azure Storage operations.

### Example Configuration

```csharp
var settings = new AzureStorageOrchestrationServiceSettings
{
    StorageConnectionString = connectionString,
    TaskHubName = "MyTaskHub",
    
    // Performance tuning
    PartitionCount = 8,
    ControlQueueBufferThreshold = 128,
    MaxConcurrentTaskOrchestrationWorkItems = 200,
    MaxConcurrentTaskActivityWorkItems = 200,
    
    // Lease settings
    LeaseInterval = TimeSpan.FromSeconds(30),
    LeaseRenewInterval = TimeSpan.FromSeconds(10)
};
```

## Architecture

### Storage Resources

The Azure Storage provider creates these resources:

| Resource Type | Name Pattern | Purpose |
| ------------- | ------------ | ------- |
| **Control Queues** | `{taskhub}-control-{0..N}` | Orchestration messages |
| **Work Item Queue** | `{taskhub}-workitems` | Activity messages |
| **History Table** | `{taskhub}History` | Orchestration history |
| **Instances Table** | `{taskhub}Instances` | Instance metadata |
| **Partitions Table** | `{taskhub}Partitions` | Partition leases (table manager) |
| **Lease Blobs** | `{taskhub}-leases/` | Partition leases (blob manager) |

### Partitioning

The Azure Storage provider uses **partitions** to distribute orchestration workloads across workers. Each partition corresponds to exactly one **control queue**.

#### How Partitioning Works

- **Orchestrations and entities** are assigned to partitions by hashing the instance ID
- Instance IDs are random GUIDs by default, ensuring even distribution across partitions
- A single orchestration instance is always processed by one partition (and therefore one worker) at a time
- The `PartitionCount` setting (1–16, default 4) determines how many control queues are created

#### Queue Architecture

The task hub uses two types of queues:

| Queue Type | Count | Purpose | Processing |
| ---------- | ----- | ------- | ---------- |
| **Control queues** | `PartitionCount` | Orchestration lifecycle messages | Partitioned — each queue owned by one worker |
| **Work item queue** | 1 | Activity function messages | Shared — all workers compete for messages |

```text
┌──────────────────────────────────────────────────────────────────────┐
│                            Task Hub                                  │
│                                                                      │
│  CONTROL QUEUES (partitioned)           WORK ITEM QUEUE (shared)     │
│  ┌──────────────────────────────────┐   ┌─────────────────────────┐  │
│  │ control-00 │ control-01 │ ...   │   │      workitems          │  │
│  │ (Worker A) │ (Worker B) │       │   │                         │  │
│  │            │            │       │   │  All workers compete    │  │
│  │ • Start    │ • Start    │       │   │  for activity messages  │  │
│  │ • Timer    │ • Timer    │       │   │                         │  │
│  │ • Activity │ • Activity │       │   └─────────────────────────┘  │
│  │   complete │   complete │       │              ▲                 │
│  │ • External │ • External │       │              │                 │
│  │   event    │   event    │       │   Activities scheduled by      │
│  └────────────┴────────────┴───────┘   orchestrators go here        │
│         │            │                                              │
│         ▼            ▼                                              │
│  ┌──────────────────────────────────┐                               │
│  │      ORCHESTRATION INSTANCES     │                               │
│  │  Hash(InstanceID) → Partition    │                               │
│  └──────────────────────────────────┘                               │
└──────────────────────────────────────────────────────────────────────┘
```

#### Control Queue Messages

Control queues contain orchestration lifecycle messages:

- **ExecutionStarted** — New orchestration started
- **TaskCompleted** — Activity function completed
- **TimerFired** — Durable timer expired
- **EventRaised** — External event received
- **SubOrchestrationCompleted** — Child orchestration completed

When messages are dequeued, up to 32 messages are fetched in a single poll. Messages for the same instance are batched together for efficient processing.

#### Work Item Queue

The work item queue is a simple, non-partitioned queue for activity function messages:

- All workers compete to dequeue activity messages
- Activities are **stateless** — any worker can execute any activity
- Activities can scale out infinitely (limited only by worker count)

#### Partition Count Guidance

| Workload | Recommended `PartitionCount` |
| -------- | ---------------------------- |
| Development/testing | 1–2 |
| Low-to-moderate throughput | 4 (default) |
| High throughput | 8–16 |

> [!IMPORTANT]
> Partition count **cannot be changed** after task hub creation. Set it high enough to accommodate future scale-out needs. The maximum number of workers that can process orchestrations concurrently equals the partition count. Note that higher partition counts increase Azure Storage costs due to more queue and table operations.

### Lease Management

Workers compete for partition ownership using one of two partition managers:

#### Table Partition Manager (Default)

When `UseTablePartitionManagement = true` (default):

- Partition leases are stored in the `{taskhub}Partitions` table
- Uses Azure Table ETags for concurrency control
- Provides better reliability due to transactional updates

#### Blob Partition Manager (Legacy)

When `UseTablePartitionManagement = false`:

- Partition leases are stored as blobs in `{taskhub}-leases/`
- Uses Azure Blob leases for concurrency control
- Available in "safe" (`UseLegacyPartitionManagement = false`) and "legacy" (`UseLegacyPartitionManagement = true`) variants

#### Partition lifecycle

1. Workers acquire leases to claim partition ownership
2. Leases are renewed at `LeaseRenewInterval` (default 10s)
3. Leases expire after `LeaseInterval` (default 30s) if not renewed
4. Partitions are automatically balanced across workers

### Message Processing

1. **Prefetching**: Messages are prefetched from control queues in batches
2. **Batching**: Messages for the same instance are grouped together
3. **History fetch**: Orchestration history is loaded from Table Storage
4. **Processing**: Orchestration code runs with the loaded history
5. **Checkpoint**: New history and messages are appended

### Checkpoint Order

Checkpoints are written in this order to ensure that no data is lost if there is a failure:

1. New messages → Storage queues
2. New history → Table storage
3. Delete processed messages

Because the checkpoints aren't atomic, duplicates may occur. The replay model handles history duplicates gracefully. Message duplicates may result in activities being executed multiple times if an unexpected failure occurs.

## Scaling

### Horizontal Scaling

Multiple workers can connect to the same task hub:

```csharp
// Worker 1, 2, 3... all connect to same task hub
var service = new AzureStorageOrchestrationService(settings);
var worker = new TaskHubWorker(service, loggerFactory);
await worker.StartAsync();
```

Partitions are automatically distributed across workers.

### Partition Count

For high-throughput scenarios, increase partition count:

```csharp
var settings = new AzureStorageOrchestrationServiceSettings
{
    PartitionCount = 16  // More partitions = more parallelism
};
```

> [!WARNING]
> Partition count cannot be changed after task hub creation.

## Operations

### Create Task Hub

```csharp
await service.CreateIfNotExistsAsync();
```

### Delete Task Hub

```csharp
await service.DeleteAsync();
```

### Purge History

```csharp
await service.PurgeOrchestrationHistoryAsync(
    DateTime.UtcNow.AddDays(-30),  // Older than 30 days
    OrchestrationStateTimeRangeFilterType.OrchestrationCompletedTimeFilter);
```

> [!WARNING]
> Purging history is very expensive and may take hours for large task hubs. It's recommended to purge history frequently and in smaller time ranges.

## Monitoring

### Azure Storage Metrics

Monitor these metrics in Azure portal:

- Queue message count
- Table transactions
- Blob lease operations

## Logging

The Azure Storage provider supports structured logging via `Microsoft.Extensions.Logging`.

### Enabling Logging

```csharp
using Microsoft.Extensions.Logging;

// Create a logger factory
ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddConsole();
    builder.AddFilter("DurableTask.AzureStorage", LogLevel.Information);
    builder.AddFilter("DurableTask.Core", LogLevel.Information);
});

// Pass logger factory to settings
var settings = new AzureStorageOrchestrationServiceSettings
{
    StorageConnectionString = connectionString,
    TaskHubName = "MyTaskHub",
    LoggerFactory = loggerFactory
};

var service = new AzureStorageOrchestrationService(settings);
```

### Log Categories

| Category | Description |
| -------- | ----------- |
| `DurableTask.AzureStorage` | Azure Storage-specific operations (messages, queues, tables) |
| `DurableTask.Core` | Core framework operations (orchestrations, activities, dispatchers) |

### Example Log Events

- `SendingMessage` / `ReceivedMessage` — Queue message operations
- `FetchedInstanceHistory` — History table reads
- `PoisonMessageDetected` — Unprocessable messages
- `PartitionManagerInfo` / `PartitionManagerWarning` — Partition management

### ETW Event Source

Events are also published to Event Tracing for Windows (ETW) via the `DurableTask-AzureStorage` event source (GUID: {4C4AD4A2-F396-5E18-01B6-618C12A10433}).

## Next Steps

- [Choosing a Backend](../getting-started/choosing-a-backend.md) — Compare all providers
- [Durable Task Scheduler](durable-task-scheduler.md) — Recommended managed alternative
- [Core Concepts](../concepts/core-concepts.md) — Learn the fundamentals
