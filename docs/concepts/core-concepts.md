# Core Concepts

This document explains the fundamental concepts of the Durable Task Framework (DTFx).

## Architecture Overview

```text
┌─────────────────────────────────────────────────────────────────┐
│                         Task Hub                                │
│  ┌─────────────────┐                    ┌─────────────────┐     │
│  │  TaskHubWorker  │                    │  TaskHubClient  │     │
│  │                 │                    │                 │     │
│  │ ┌─────────────┐ │                    │ • Start         │     │
│  │ │Orchestration│ │                    │ • Query         │     │
│  │ │  Handlers   │ │                    │ • Send Events   │     │
│  │ └─────────────┘ │                    │ • Terminate     │     │
│  │ ┌─────────────┐ │                    │                 │     │
│  │ │  Activity   │ │                    └────────┬────────┘     │
│  │ │  Handlers   │ │                             │              │
│  │ └─────────────┘ │                             │              │
│  └────────┬────────┘                             │              │
│           │                                      │              │
│           └──────────────────┬───────────────────┘              │
│                              │                                  │
│                              ▼                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │               IOrchestrationService                       │  │
│  │                   (Backend Provider)                      │  │
│  │                                                           │  │
│  │   • Message Queues (control, work items)                  │  │
│  │   • State Storage (history, instances)                    │  │
│  │   • Scale Management (partitions, etc.)                   │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Task Hub

A **Task Hub** is a logical container for orchestration and activity state. It represents a single deployment unit and includes:

- **Message Queues** — Queues for orchestration and activity work items
- **History Store** — Persistent storage for orchestration history
- **Instance Store** — Metadata about orchestration instances for querying

### Key Characteristics

- Each task hub is **isolated** — orchestrations in different task hubs cannot interact directly
- Multiple workers can connect to the same task hub for **scale-out**
- All connected workers must share the same backend provider configuration and orchestration/activity code
- The task hub name is used as a **namespace** for all stored data

## TaskHubWorker

The **TaskHubWorker** hosts and executes orchestrations and activities. It:

- Polls the backend for work items
- Dispatches orchestration and activity code
- Reports completion back to the backend

### Lifecycle

```csharp
// Create worker
var orchestrationService = GetSelectedOrchestrationService();
var worker = new TaskHubWorker(orchestrationService, loggerFactory);

// Register handlers
worker.AddTaskOrchestrations(typeof(MyOrchestration));
worker.AddTaskActivities(typeof(MyActivity));

// Start processing
await worker.StartAsync();

// ... application runs ...

// Graceful shutdown
await worker.StopAsync();
```

### Scaling

Multiple workers can connect to the same task hub:

```text
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│   Worker 1  │  │   Worker 2  │  │   Worker 3  │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       └────────────────┼────────────────┘
                        │
                        ▼
              ┌─────────────────┐
              │    Task Hub     │
              └─────────────────┘
```

Work is distributed across workers automatically by the selected backend provider.

## TaskHubClient

The **TaskHubClient** is used to manage orchestration instances from external code:

```csharp
var orchestrationService = GetSelectedOrchestrationService();
var client = new TaskHubClient(orchestrationService, loggerFactory: loggerFactory);

// Start a new orchestration
var instance = await client.CreateOrchestrationInstanceAsync(
    typeof(MyOrchestration),
    instanceId: "order-123",
    input: new OrderData { ... });

// Query status
var state = await client.GetOrchestrationStateAsync(instance);

// Send an event
await client.RaiseEventAsync(instance, "ApprovalReceived", approvalData);

// Wait for completion
var result = await client.WaitForOrchestrationAsync(instance, timeout);

// Terminate
await client.TerminateInstanceAsync(instance, "Cancelled by user");
```

## Orchestrations

**Orchestrations** are the core workflow definitions. They:

- Define the sequence and logic of work
- Coordinate activities and sub-orchestrations
- Are **durable** — survive process restarts
- Must be **deterministic** — same input produces same sequence of actions

```csharp
public class OrderOrchestration : TaskOrchestration<OrderResult, OrderInput>
{
    public override async Task<OrderResult> RunTask(
        OrchestrationContext context, 
        OrderInput input)
    {
        // Orchestration logic here
        var validated = await context.ScheduleTask<bool>(typeof(ValidateOrder), input);
        
        if (!validated)
            return new OrderResult { Success = false };
            
        await context.ScheduleTask<string>(typeof(ProcessPayment), input);
        await context.ScheduleTask<string>(typeof(ShipOrder), input);
        
        return new OrderResult { Success = true };
    }
}
```

See [Orchestrations](orchestrations.md) for detailed documentation.

## Activities

**Activities** are the units of work that orchestrations schedule. They:

- Perform the actual work (API calls, database operations, etc.)
- Can be **retried** automatically on failure
- Are **not** required to be deterministic
- Run once per scheduled invocation (with at-least-once guarantees)

```csharp
public class ProcessPaymentActivity : AsyncTaskActivity<PaymentInput, PaymentResult>
{
    protected override async Task<PaymentResult> ExecuteAsync(
        TaskContext context, 
        PaymentInput input)
    {
        // Actual work here - call payment API, etc.
        var result = await PaymentService.ProcessAsync(input);
        return result;
    }
}
```

See [Activities](activities.md) for detailed documentation.

## Instance IDs

Every orchestration instance has a unique **Instance ID**:

```csharp
// Auto-generated ID
var instance = await client.CreateOrchestrationInstanceAsync(typeof(MyOrchestration), input);
// instance.InstanceId = "abc123..."

// Custom ID (recommended for idempotency)
var instance = await client.CreateOrchestrationInstanceAsync(
    typeof(MyOrchestration),
    instanceId: "order-456",  // Your custom ID
    input: orderData);
```

### Best Practices

- Use **meaningful IDs** like `order-{orderId}` or `user-{userId}-workflow`
- Use random GUIDs if no meaningful ID is available and make sure to store them
- Avoid reusing IDs for different logical workflows to prevent conflicts

## Orchestration Status

Orchestrations can be in one of these states:

| Status | Description |
| ------ | ----------- |
| `Pending` | Scheduled but not yet started |
| `Running` | Currently executing or waiting |
| `Suspended` | Paused due to external request |
| `Completed` | Finished successfully |
| `Failed` | Terminated due to unhandled exception |
| `Terminated` | Explicitly terminated via API |
| `Canceled` | Not currently implemented |
| `ContinuedAsNew` | Restarted via `ContinueAsNew` (not used in recent versions) |

```csharp
var state = await client.GetOrchestrationStateAsync(instance);
Console.WriteLine($"Status: {state.OrchestrationStatus}");
```

## Next Steps

- [Orchestrations](orchestrations.md) — Writing orchestration logic
- [Activities](activities.md) — Writing activity code
- [Replay and Durability](replay-and-durability.md) — How durability works
- [Deterministic Constraints](deterministic-constraints.md) — Rules for orchestration code
