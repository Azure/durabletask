# Sub-Orchestrations

Sub-orchestrations allow you to break complex workflows into smaller, reusable pieces. A parent orchestration can start child orchestrations and wait for their results.

## Creating Sub-Orchestrations

### Basic Usage

```csharp
public override async Task<OrderResult> RunTask(
    OrchestrationContext context, 
    OrderInput input)
{
    // Start a sub-orchestration and wait for result
    var paymentResult = await context.CreateSubOrchestrationInstance<PaymentResult>(
        typeof(PaymentOrchestration),
        input.PaymentData);
    
    return new OrderResult { PaymentId = paymentResult.TransactionId };
}
```

### With Custom Instance ID

```csharp
var result = await context.CreateSubOrchestrationInstance<ShippingResult>(
    typeof(ShippingOrchestration),
    instanceId: $"shipping-{input.OrderId}",  // Custom ID
    input: input.ShippingData);
```

### With Retry Options

```csharp
var retryOptions = new RetryOptions(
    firstRetryInterval: TimeSpan.FromSeconds(30),
    maxNumberOfAttempts: 3)
{
    BackoffCoefficient = 2.0
};

var result = await context.CreateSubOrchestrationInstanceWithRetry<Result>(
    typeof(ChildOrchestration),
    retryOptions,
    input);
```

## Sub-Orchestration Patterns

### Sequential Sub-Orchestrations

```csharp
public override async Task<OrderResult> RunTask(
    OrchestrationContext context, 
    OrderInput input)
{
    // Step 1: Validate
    var validationResult = await context.CreateSubOrchestrationInstance<ValidationResult>(
        typeof(ValidationOrchestration),
        input);
    
    if (!validationResult.IsValid)
        return new OrderResult { Error = validationResult.Error };
    
    // Step 2: Process payment
    var paymentResult = await context.CreateSubOrchestrationInstance<PaymentResult>(
        typeof(PaymentOrchestration),
        input.PaymentData);
    
    // Step 3: Fulfill order
    var fulfillmentResult = await context.CreateSubOrchestrationInstance<FulfillmentResult>(
        typeof(FulfillmentOrchestration),
        new FulfillmentInput { OrderId = input.OrderId, PaymentId = paymentResult.Id });
    
    return new OrderResult { Success = true, TrackingNumber = fulfillmentResult.TrackingNumber };
}
```

### Parallel Sub-Orchestrations (Fan-Out)

```csharp
public override async Task<BatchResult> RunTask(
    OrchestrationContext context, 
    BatchInput input)
{
    // Start all sub-orchestrations in parallel
    var tasks = input.Items.Select(item =>
        context.CreateSubOrchestrationInstance<ItemResult>(
            typeof(ProcessItemOrchestration),
            instanceId: $"item-{item.Id}",
            input: item));
    
    // Wait for all to complete
    var results = await Task.WhenAll(tasks);
    
    return new BatchResult
    {
        ProcessedCount = results.Length,
        SuccessCount = results.Count(r => r.Success),
        FailedItems = results.Where(r => !r.Success).Select(r => r.ItemId).ToList()
    };
}
```

### Conditional Sub-Orchestrations

```csharp
public override async Task<Result> RunTask(
    OrchestrationContext context, 
    Input input)
{
    // Choose sub-orchestration based on input
    if (input.ProcessType == ProcessType.Express)
    {
        return await context.CreateSubOrchestrationInstance<Result>(
            typeof(ExpressProcessingOrchestration),
            input);
    }
    else
    {
        return await context.CreateSubOrchestrationInstance<Result>(
            typeof(StandardProcessingOrchestration),
            input);
    }
}
```

### Hierarchical Workflows

```csharp
// Top-level orchestration
public class ProjectOrchestration : TaskOrchestration<ProjectResult, ProjectInput>
{
    public override async Task<ProjectResult> RunTask(
        OrchestrationContext context, 
        ProjectInput input)
    {
        var results = new List<PhaseResult>();
        
        foreach (var phase in input.Phases)
        {
            var phaseResult = await context.CreateSubOrchestrationInstance<PhaseResult>(
                typeof(PhaseOrchestration),
                phase);
            results.Add(phaseResult);
            
            if (!phaseResult.Success)
                break;  // Stop on failure
        }
        
        return new ProjectResult { Phases = results };
    }
}

// Second-level orchestration
public class PhaseOrchestration : TaskOrchestration<PhaseResult, PhaseInput>
{
    public override async Task<PhaseResult> RunTask(
        OrchestrationContext context, 
        PhaseInput input)
    {
        // Each phase has multiple tasks as sub-orchestrations
        var taskResults = await Task.WhenAll(
            input.Tasks.Select(t =>
                context.CreateSubOrchestrationInstance<TaskResult>(
                    typeof(TaskOrchestration),
                    t)));
        
        return new PhaseResult
        {
            Success = taskResults.All(r => r.Success),
            TaskResults = taskResults.ToList()
        };
    }
}
```

## Sub-Orchestration vs Activity

| Feature | Sub-Orchestration | Activity |
| ------- | ----------------- | -------- |
| **Can call other orchestrations** | ✅ Yes | ❌ Not directly |
| **Can use timers** | ✅ Yes | ❌ No |
| **Can wait for events** | ✅ Yes | ❌ No |
| **Has own history** | ✅ Yes | ❌ No |
| **Overhead** | Higher | Lower |
| **Use for** | Complex workflows | Single operations |

### When to Use Sub-Orchestrations

✅ **Use sub-orchestrations when:**

- The child workflow needs timers, events, or fan-out
- You want isolated history (for debugging, monitoring, or to reduce parent history size)
- You want to distribute orchestration work across multiple worker instances
- The logic is reusable across multiple parent orchestrations
- The child workflow is complex enough to warrant separate management

❌ **Use activities instead when:**

- Performing a single operation (API call, DB query)
- No need for durable timers or events
- Simple, stateless work

## Instance ID Management

### Auto-Generated IDs

```csharp
// ID is an automatically generated GUID
var result = await context.CreateSubOrchestrationInstance<Result>(
    typeof(ChildOrchestration),
    input);
```

### Custom IDs for Idempotency

```csharp
// Using custom ID ensures idempotency
var result = await context.CreateSubOrchestrationInstance<Result>(
    typeof(ChildOrchestration),
    instanceId: $"{context.OrchestrationInstance.InstanceId}:child:{input.ItemId}",
    input: input);
```

### Naming Conventions

```csharp
// Good patterns for sub-orchestration IDs:
$"{parentId}:payment"                    // Single child of type
$"{parentId}:item:{itemId}"              // Multiple children by item
$"order-{orderId}:fulfillment"           // Business-meaningful
```

## Error Handling

### Catching Sub-Orchestration Failures

```csharp
public override async Task<Result> RunTask(OrchestrationContext context, Input input)
{
    try
    {
        var result = await context.CreateSubOrchestrationInstance<Result>(
            typeof(RiskyOrchestration),
            input);
        return result;
    }
    catch (SubOrchestrationFailedException ex)
    {
        // Sub-orchestration threw an unhandled exception
        await context.ScheduleTask<bool>(typeof(CompensationActivity), input);
        return new Result { Error = ex.Message };
    }
}
```

### Timeout Handling

```csharp
public override async Task<Result> RunTask(OrchestrationContext context, Input input)
{
    using var cts = new CancellationTokenSource();
    
    var subOrchTask = context.CreateSubOrchestrationInstance<Result>(
        typeof(LongRunningOrchestration),
        input);
    
    var timeoutTask = context.CreateTimer(
        context.CurrentUtcDateTime.AddHours(1),
        true,
        cts.Token);
    
    var winner = await Task.WhenAny(subOrchTask, timeoutTask);
    
    if (winner == subOrchTask)
    {
        cts.Cancel();
        return await subOrchTask;
    }
    else
    {
        // Note: The sub-orchestration continues running!
        // Consider terminating it via an activity if needed
        return new Result { TimedOut = true };
    }
}
```

## Monitoring Sub-Orchestrations

### Getting Sub-Orchestration Status

```csharp
// From an activity or external code
var service = GetOrchestrationService();
var client = new TaskHubClient(service, loggerFactory: loggerFactory);
var state = await client.GetOrchestrationStateAsync(
    new OrchestrationInstance { InstanceId = subOrchestrationId });
```

### Viewing in History

Sub-orchestration events in parent history:

```text
SubOrchestrationInstanceCreated { InstanceId: "child-123", Name: "PaymentOrchestration" }
SubOrchestrationInstanceCompleted { InstanceId: "child-123", Result: "{...}" }
```

## Next Steps

- [Activities](../concepts/activities.md) — When to use activities instead
- [Error Handling](error-handling.md) — Handling sub-orchestration failures
- [Versioning](versioning.md) — Updating sub-orchestration code
