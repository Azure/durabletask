# Custom Provider Implementation

You can implement a custom storage provider by implementing the `IOrchestrationService` interface. This allows you to use DTFx with any backend storage system.

## When to Implement a Custom Provider

✅ **Good for:**

- Integrating with proprietary storage systems
- Specialized requirements not met by existing providers
- Research and experimentation

⚠️ **Consider existing providers first:**

- [Durable Task Scheduler](durable-task-scheduler.md) — Managed service
- [Azure Storage](azure-storage.md) — Self-managed with Azure Storage
- [Emulator](emulator.md) — Local development

## Core Interfaces

### IOrchestrationService

The primary interface for storage providers:

```csharp
public interface IOrchestrationService
{
    // Lifecycle
    Task StartAsync();
    Task StopAsync();
    Task StopAsync(bool isForced);
    Task CreateAsync();
    Task CreateAsync(bool recreateInstanceStore);
    Task CreateIfNotExistsAsync();
    Task DeleteAsync();
    Task DeleteAsync(bool deleteInstanceStore);
    
    // Orchestration dispatcher
    int TaskOrchestrationDispatcherCount { get; }
    int MaxConcurrentTaskOrchestrationWorkItems { get; }
    BehaviorOnContinueAsNew EventBehaviourForContinueAsNew { get; }
    
    bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState);
    int GetDelayInSecondsAfterOnProcessException(Exception exception);
    int GetDelayInSecondsAfterOnFetchException(Exception exception);
    
    Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(
        TimeSpan receiveTimeout, CancellationToken cancellationToken);
    Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem);
    Task CompleteTaskOrchestrationWorkItemAsync(
        TaskOrchestrationWorkItem workItem,
        OrchestrationRuntimeState newOrchestrationRuntimeState,
        IList<TaskMessage> outboundMessages,
        IList<TaskMessage> orchestratorMessages,
        IList<TaskMessage> timerMessages,
        TaskMessage continuedAsNewMessage,
        OrchestrationState orchestrationState);
    Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem);
    Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem);
    
    // Activity dispatcher
    int TaskActivityDispatcherCount { get; }
    int MaxConcurrentTaskActivityWorkItems { get; }
    
    Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(
        TimeSpan receiveTimeout, CancellationToken cancellationToken);
    Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem);
    Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage);
    Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem);
}
```

### IOrchestrationServiceClient

For client operations (starting, querying, managing instances):

```csharp
public interface IOrchestrationServiceClient
{
    Task CreateTaskOrchestrationAsync(TaskMessage creationMessage);
    Task CreateTaskOrchestrationAsync(TaskMessage creationMessage, OrchestrationStatus[] dedupeStatuses);
    
    Task SendTaskOrchestrationMessageAsync(TaskMessage message);
    Task SendTaskOrchestrationMessageBatchAsync(params TaskMessage[] messages);
    
    Task<OrchestrationState> WaitForOrchestrationAsync(
        string instanceId,
        string executionId,
        TimeSpan timeout,
        CancellationToken cancellationToken);
    
    Task ForceTerminateTaskOrchestrationAsync(string instanceId, string reason);
    
    Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId);
    Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions);
    
    Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId);
    Task PurgeOrchestrationHistoryAsync(
        DateTime thresholdDateTimeUtc,
        OrchestrationStateTimeRangeFilterType timeRangeFilterType);
}
```

> [!NOTE]
> Most providers implement both interfaces in a single class.

## Minimal Implementation

Here's a skeleton for a custom provider:

```csharp
public class MyCustomOrchestrationService : IOrchestrationService, IOrchestrationServiceClient
{
    private readonly MyStorageBackend _storage;
    
    public MyCustomOrchestrationService(string connectionString)
    {
        _storage = new MyStorageBackend(connectionString);
    }
    
    // Lifecycle
    public Task CreateAsync() => CreateIfNotExistsAsync();
    public Task CreateAsync(bool recreateInstanceStore) => CreateIfNotExistsAsync();
    
    public async Task CreateIfNotExistsAsync()
    {
        await _storage.InitializeAsync();
    }
    
    public async Task DeleteAsync()
    {
        await _storage.DeleteAllDataAsync();
    }
    
    public Task DeleteAsync(bool deleteInstanceStore) => DeleteAsync();
    
    // Worker lifecycle
    public Task StartAsync()
    {
        // Start background processes if needed
        return Task.CompletedTask;
    }
    
    public Task StopAsync() => StopAsync(false);
    public Task StopAsync(bool isForced)
    {
        // Stop background processes
        return Task.CompletedTask;
    }
    
    // Work item polling
    public async Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(
        TimeSpan receiveTimeout,
        CancellationToken cancellationToken)
    {
        // Poll for orchestration messages
        var message = await _storage.DequeueOrchestrationMessageAsync(receiveTimeout, cancellationToken);
        if (message == null) return null;
        
        // Load history
        var history = await _storage.LoadHistoryAsync(message.InstanceId);
        
        return new TaskOrchestrationWorkItem
        {
            InstanceId = message.InstanceId,
            NewMessages = new[] { message },
            OrchestrationRuntimeState = new OrchestrationRuntimeState(history)
        };
    }
    
    public async Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(
        TimeSpan receiveTimeout,
        CancellationToken cancellationToken)
    {
        var message = await _storage.DequeueActivityMessageAsync(receiveTimeout, cancellationToken);
        if (message == null) return null;
        
        return new TaskActivityWorkItem
        {
            Id = Guid.NewGuid().ToString(),
            TaskMessage = message
        };
    }
    
    // Orchestration completion
    public async Task CompleteTaskOrchestrationWorkItemAsync(
        TaskOrchestrationWorkItem workItem,
        OrchestrationRuntimeState newState,
        IList<TaskMessage> outboundMessages,
        IList<TaskMessage> orchestratorMessages,
        IList<TaskMessage> timerMessages,
        TaskMessage continuedAsNewMessage,
        OrchestrationState state)
    {
        // Save new history
        await _storage.SaveHistoryAsync(workItem.InstanceId, newState.Events);
        
        // Enqueue outbound messages (activities)
        foreach (var msg in outboundMessages)
        {
            await _storage.EnqueueActivityMessageAsync(msg);
        }
        
        // Enqueue orchestrator messages (sub-orchestrations, events)
        foreach (var msg in orchestratorMessages)
        {
            await _storage.EnqueueOrchestrationMessageAsync(msg);
        }
        
        // Handle timers
        foreach (var msg in timerMessages)
        {
            await _storage.ScheduleTimerAsync(msg);
        }
        
        // Handle continue-as-new
        if (continuedAsNewMessage != null)
        {
            await _storage.EnqueueOrchestrationMessageAsync(continuedAsNewMessage);
        }
        
        // Update instance status
        await _storage.UpdateInstanceStateAsync(workItem.InstanceId, state);
    }
    
    // ... implement remaining interface methods
    
    // Capabilities
    public int TaskOrchestrationDispatcherCount => 1;
    public int MaxConcurrentTaskOrchestrationWorkItems => settings.MaxOrchestrationConcurrency;
    public int TaskActivityDispatcherCount => 1;
    public int MaxConcurrentTaskActivityWorkItems => settings.MaxActivityConcurrency;
    public BehaviorOnContinueAsNew EventBehaviourForContinueAsNew => 
        BehaviorOnContinueAsNew.Carryover;
}
```

## Key Concepts

### Work Items

**TaskOrchestrationWorkItem**: Represents orchestration work to process

- Contains one or more messages that triggered the orchestrator invocation (`ExecutionStartedEvent`, `TaskCompletedEvent`, etc.)
- Contains the current orchestration state (the full history)
- Must be completed or abandoned, and should always be released

**TaskActivityWorkItem**: Represents activity work to execute

- Contains a single activity task message (`TaskScheduledEvent`)
- Must be completed or abandoned
- Supports lock renewal for long-running activities

### State Management

Your provider must manage:

1. **Message queues** — For orchestration and activity messages
2. **History storage** — For orchestration event history
3. **Instance metadata** — For querying orchestration status
4. **Timer scheduling** — For durable timers

How you implement these components is entirely up to you. In the ideal case, your storage backend should provide atomic operations (like in the Durable Task Scheduler and MSSQL backend providers) to ensure consistency. If that's not possible (like in Azure Storage), you must gracefully handle potential inconsistencies due to process crashes to ensure there's no data loss.

## Next Steps

- [Azure Storage Provider](azure-storage.md) — Reference implementation
- [Core Concepts](../concepts/core-concepts.md) — Understand the architecture
- [Replay and Durability](../concepts/replay-and-durability.md) — Key concepts for providers
