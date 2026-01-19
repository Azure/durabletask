# Service Fabric Provider

The Service Fabric provider uses Azure Service Fabric reliable collections for orchestration state. It's designed for applications already running on Service Fabric clusters.

## Installation

```bash
dotnet add package Microsoft.Azure.DurableTask.AzureServiceFabric
```

## Configuration

### Basic Setup

The Service Fabric provider includes built-in infrastructure via `TaskHubStatefulService` and `TaskHubProxyListener`:

```csharp
using DurableTask.AzureServiceFabric;
using DurableTask.AzureServiceFabric.Service;
using DurableTask.Core;
using Microsoft.ServiceFabric.Services.Runtime;

// In Program.cs
ServiceRuntime.RegisterServiceAsync("StatefulServiceType", context =>
{
    var settings = new FabricOrchestrationProviderSettings();
    
    var listener = new TaskHubProxyListener(
        settings,
        RegisterOrchestrations);
    
    return new TaskHubStatefulService(context, new[] { listener });
}).GetAwaiter().GetResult();

void RegisterOrchestrations(TaskHubWorker worker)
{
    worker.AddTaskOrchestrations(typeof(MyOrchestration));
    worker.AddTaskActivities(typeof(MyActivity));
}
```

### Manual Setup with Provider Factory

For more control, use the `FabricOrchestrationProviderFactory`:

```csharp
using DurableTask.AzureServiceFabric;
using DurableTask.Core;
using Microsoft.ServiceFabric.Services.Runtime;

public class DurableTaskService : StatefulService
{
    private FabricOrchestrationProvider provider;
    private TaskHubWorker worker;
    
    public DurableTaskService(StatefulServiceContext context) : base(context) { }
    
    protected override async Task RunAsync(CancellationToken cancellationToken)
    {
        var settings = new FabricOrchestrationProviderSettings();
        
        var factory = new FabricOrchestrationProviderFactory(
            this.StateManager,
            settings);
        
        provider = factory.CreateProvider();
        
        worker = new TaskHubWorker(provider.OrchestrationService, settings.LoggerFactory);
        worker.AddTaskOrchestrations(typeof(MyOrchestration));
        worker.AddTaskActivities(typeof(MyActivity));
        
        await worker.StartAsync();
        
        try
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }
        finally
        {
            await worker.StopAsync();
            provider.Dispose();
        }
    }
}
```

### Service Registration

Register the service in `Program.cs`:

```csharp
ServiceRuntime.RegisterServiceAsync(
    "DurableTaskServiceType",
    context => new DurableTaskService(context))
    .GetAwaiter().GetResult();
```

## Architecture

### Reliable Collections

State is stored in Service Fabric reliable collections:

| Collection Name | Purpose |
| --------------- | ------- |
| `DtfxSfp_Orchestrations` | Orchestration sessions and state |
| `DtfxSfp_Activities` | Pending activity messages |
| `DtfxSfp_InstanceStore` | Instance metadata for queries |
| `DtfxSfp_ExecutionIdStore` | Execution ID mappings |
| `DtfxSfp_ScheduledMessages` | Scheduled timer messages |
| `DtfxSfp_SessionMessages_{id}` | Per-session message queues |

### Partitioning

Service Fabric handles partitioning automatically based on your service configuration:

```xml
<Service Name="DurableTaskService">
  <StatefulService ServiceTypeName="DurableTaskServiceType">
    <UniformInt64Partition PartitionCount="4" LowKey="0" HighKey="3" />
  </StatefulService>
</Service>
```

## Configuration Options

| Setting | Description | Default |
| ------- | ----------- | ------- |
| `TaskOrchestrationDispatcherSettings.MaxConcurrentOrchestrations` | Max concurrent orchestrations | 1000 |
| `TaskOrchestrationDispatcherSettings.DispatcherCount` | Number of orchestration dispatchers | 10 |
| `TaskActivityDispatcherSettings.MaxConcurrentActivities` | Max concurrent activities | 1000 |
| `TaskActivityDispatcherSettings.DispatcherCount` | Number of activity dispatchers | 10 |
| `LoggerFactory` | Optional logger factory for diagnostics | null |

### Example Configuration

```csharp
var settings = new FabricOrchestrationProviderSettings
{
    TaskOrchestrationDispatcherSettings =
    {
        MaxConcurrentOrchestrations = 500,
        DispatcherCount = 5
    },
    TaskActivityDispatcherSettings =
    {
        MaxConcurrentActivities = 500,
        DispatcherCount = 5
    }
};
```

## Client Access

### From Within Service Fabric

Use the `FabricOrchestrationProvider` to get both worker and client:

```csharp
var factory = new FabricOrchestrationProviderFactory(this.StateManager, settings);
var provider = factory.CreateProvider();

var worker = new TaskHubWorker(provider.OrchestrationService, settings.LoggerFactory);
var client = new TaskHubClient(provider.OrchestrationServiceClient, loggerFactory: settings.LoggerFactory);

var instance = await client.CreateOrchestrationInstanceAsync(
    typeof(MyOrchestration),
    input);
```

### From External Applications

External clients connect via the built-in HTTP API or Service Fabric remoting:

```csharp
// Create a service proxy
var serviceUri = new Uri("fabric:/MyApp/DurableTaskService");
var proxy = ServiceProxy.Create<IMyDurableTaskService>(serviceUri);

// Call methods on the proxy
var instanceId = await proxy.StartOrchestrationAsync(input);
```

The `TaskHubProxyListener` exposes an HTTP API via `FabricOrchestrationServiceController` for external access.

## Limitations

- Requires Service Fabric cluster
- Tightly coupled to Service Fabric ecosystem
- More complex deployment and management
- No external persistence — state is lost if all replicas are lost

## Next Steps

- [Choosing a Backend](../getting-started/choosing-a-backend.md) — Compare all providers
- [Durable Task Scheduler](durable-task-scheduler.md) — Recommended managed alternative
- [Azure Storage Provider](azure-storage.md) — Alternative self-managed option
