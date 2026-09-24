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
| **Worker Partition Metadata** | `durabletask_taskhub` metadata on `{taskhub}-workitems` | Complete partition configuration published during explicit hub initialization |

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

#### Clients targeting another worker's hub

Explicit hub initialization (`CreateIfNotExistsAsync`, `CreateAsync`, or worker startup) publishes the worker's configured partition count before creating partition leases. Client operations use that published count for queue initialization and message routing, rather than applying the caller's `PartitionCount` to an existing target hub. Clients do not publish this metadata or modify partition leases.

> [!WARNING]
> **Breaking change:** Client operations now require published worker partition metadata. Client-only automatic hub creation is no longer supported. Missing metadata is rejected even if the client's partition count matches the target's. Existing hubs initialized only by older workers must be explicitly initialized with an upgraded provider before these clients can use them.

Deploy the upgraded target worker first and let it publish metadata, or explicitly call `CreateIfNotExistsAsync` using the target's existing partition configuration, before issuing client operations. `CreateAsync` and worker startup also remain able to initialize a hub. Do not change an existing hub's partition count during this upgrade.

If the work-item queue or its `durabletask_taskhub` metadata entry is absent, the client throws an actionable exception without creating queues, tables, blobs, or leases or enqueuing the message. This includes calls racing with worker startup before metadata publication. Failed initialization is not cached: retrying the same client after publication succeeds. There is no fallback to the caller's count or a partially populated partition table. Malformed metadata and storage access failures remain explicit errors.

The same precondition applies to history reads, purge operations, and large-message downloads. A timed purge includes initialization in its deadline; if the deadline expires first, it returns zero deleted instances with `IsComplete = false` and does not later start purging. Shared initialization is not cancelled for other callers and may continue provisioning resources once valid metadata is available. This is a no-late-purge guarantee, not cancellation of all shared initialization work.

Both worker and client binaries must be upgraded to benefit from this behavior. Older client binaries ignore the metadata and are not fixed by upgrading the worker alone. Successfully initialized clients cache topology; recreate them after deleting and recreating a hub externally. Explicit worker initialization preserves its already-completed initialization path for local clients and activity admission.

The hub creation APIs remain administrative operations that apply their configured worker settings; they should not be used to discover another worker's configuration. Hub deletion removes the metadata along with the existing work-item queue, including when deletion is performed by an older provider. Unlike best-effort app-lease cleanup, work-item queue deletion failures are propagated.

When worker metadata is already published, explicit initialization and worker startup validate it before changing resources. A different configured partition count or invalid existing metadata is rejected; a matching marker, including its creation timestamp and unrelated queue metadata, is preserved. Destructive `CreateAsync()` still deletes and recreates a hub and can therefore establish a different configuration after deletion.

This guard does not infer a legacy hub's complete topology from lease rows. Explicit initialization of an older unmarked hub still requires the operator to supply its correct existing partition configuration. All concurrently initializing hosts must agree on that configuration. Queue metadata read/validation/publication is not a compare-and-swap protocol; conflicting first-time publishers with different counts are not a supported deployment. The guard protects a count already present when read, not arbitrary conflicting first-publication races.

Client initialization retains references to every discovered control queue. Deleting the hub through that service therefore removes the full discovered topology, including queues the client has never sent to, even when the caller's configured partition count is smaller.

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

Safe-mode intent and ownership leases use different prefixes in the same container. Hub deletion deletes that shared container once, not once per prefix. An already-absent container is handled idempotently; other storage deletion failures are propagated.

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
