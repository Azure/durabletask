# Activities

Activities are the basic units of work in the Durable Task Framework. They perform actual operations like calling APIs, accessing databases, or performing computations. Unlike orchestrations, activities do not need to be deterministic.

## Creating Activities

### Type Parameters

- `TInput` — The input type passed from the orchestration
- `TResult` — The return type sent back to the orchestration

Note that input and output types must be JSON-serializable. See [serialization](../advanced/serialization.md) for details.

### Synchronous Activities

For simple, synchronous work:

```csharp
using DurableTask.Core;

public class GreetActivity : TaskActivity<string, string>
{
    protected override string Execute(TaskContext context, string name)
    {
        return $"Hello, {name}!";
    }
}
```

### Asynchronous Activities

For async operations (recommended for I/O):

```csharp
public class CallApiActivity : AsyncTaskActivity<ApiRequest, ApiResponse>
{
    private static readonly HttpClient s_httpClient = new HttpClient();
    
    protected override async Task<ApiResponse> ExecuteAsync(
        TaskContext context, 
        ApiRequest input)
    {
        using var response = await s_httpClient.PostAsJsonAsync(input.Url, input.Body);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<ApiResponse>();
    }
}
```

## Registration

### Basic Registration

```csharp
var worker = new TaskHubWorker(service, loggerFactory);
worker.AddTaskActivities(typeof(GreetActivity), typeof(CallApiActivity));
await worker.StartAsync();
```

### With Dependency Injection

Create activity instances with dependencies:

```csharp
// Using activity factory
worker.AddTaskActivities(new ActivityObjectCreator<CallApiActivity>(
    () => new CallApiActivity(httpClient)));

// Or implement INameVersionObjectManager<TaskActivity> for full control
```

### With Generic Creator

```csharp
public class MyActivityCreator : ObjectCreator<TaskActivity>
{
    private readonly IServiceProvider _services;
    
    public MyActivityCreator(IServiceProvider services)
    {
        _services = services;
    }
    
    public override TaskActivity Create()
    {
        // Resolve from DI container
        return (TaskActivity)_services.GetRequiredService(Type);
    }
}

// Register
worker.AddTaskActivities(new MyActivityCreator(serviceProvider));
```

## Calling Activities from Orchestrations

### Basic Call

```csharp
var result = await context.ScheduleTask<string>(typeof(GreetActivity), "World");
```

### With Retry Options

```csharp
var retryOptions = new RetryOptions(
    firstRetryInterval: TimeSpan.FromSeconds(5),
    maxNumberOfAttempts: 3)
{
    BackoffCoefficient = 2.0,
    MaxRetryInterval = TimeSpan.FromMinutes(1),
    RetryTimeout = TimeSpan.FromMinutes(10)
};

var result = await context.ScheduleWithRetry<string>(
    typeof(CallApiActivity),
    retryOptions,
    apiRequest);
```

### Using Typed Proxies

Generate strongly-typed activity clients:

```csharp
// Define interface
public interface IOrderActivities
{
    Task<bool> ValidateOrder(Order order);
    Task<PaymentResult> ProcessPayment(PaymentRequest request);
    Task<string> ShipOrder(ShippingRequest request);
}

// In orchestration
public override async Task<OrderResult> RunTask(
    OrchestrationContext context, 
    Order order)
{
    var activities = context.CreateClient<IOrderActivities>();
    
    var isValid = await activities.ValidateOrder(order);
    if (!isValid) return new OrderResult { Success = false };
    
    var payment = await activities.ProcessPayment(order.Payment);
    var tracking = await activities.ShipOrder(order.Shipping);
    
    return new OrderResult { Success = true, TrackingNumber = tracking };
}
```

> [!IMPORTANT]
> Do not include `TaskContext` or `CancellationToken` parameters in activity interface methods. Only JSON-serializable input and output types are allowed.

## Activity Best Practices

### 1. Keep Activities Focused

Each activity should do one thing:

```csharp
// ✅ Good - single responsibility
public class SendEmailActivity : AsyncTaskActivity<EmailMessage, bool> { }
public class SaveToDbActivity : AsyncTaskActivity<DbRecord, int> { }

// ❌ Bad - too many responsibilities
public class DoEverythingActivity : AsyncTaskActivity<Input, Output>
{
    // Sends email, saves to DB, calls API, etc.
}
```

The exception to this is when performance considerations require batching multiple related operations together to reduce overhead. However, this must be done carefully with attention to error handling and idempotency.

### 2. Make Activities Idempotent

Activities may be retried, so design them to be idempotent:

```csharp
public class ProcessPaymentActivity : AsyncTaskActivity<PaymentRequest, PaymentResult>
{
    protected override async Task<PaymentResult> ExecuteAsync(
        TaskContext context, 
        PaymentRequest input)
    {
        // Use idempotency key to prevent duplicate charges
        return await _paymentService.ProcessAsync(
            input, 
            idempotencyKey: input.OrderId);
    }
}
```

### 3. Handle Timeouts

Implement cancellation support:

```csharp
public class LongRunningActivity : AsyncTaskActivity<Input, Output>
{
    protected override async Task<Output> ExecuteAsync(
        TaskContext context, 
        Input input)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        
        try
        {
            return await DoWorkAsync(input, cts.Token);
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException("Activity timed out");
        }
    }
}
```

### 4. Log with Context

Include orchestration context in logs:

```csharp
public class MyActivity : AsyncTaskActivity<Input, Output>
{
    private readonly ILogger<MyActivity> _logger;
    
    protected override async Task<Output> ExecuteAsync(
        TaskContext context, 
        Input input)
    {
        _logger.LogInformation(
            "Processing {Input} for orchestration {InstanceId}",
            input,
            context.OrchestrationInstance.InstanceId);
        
        // ... do work ...
    }
}
```

### 5. Return Serializable Results

Ensure return types can be serialized:

```csharp
// ✅ Good - serializable POCO
public class ActivityResult
{
    public string Status { get; set; }
    public int Count { get; set; }
    public DateTime ProcessedAt { get; set; }
}

// ❌ Bad - not serializable
public class BadResult
{
    public HttpClient Client { get; set; }  // Can't serialize
    public Stream DataStream { get; set; }  // Can't serialize
}
```

## Activity Execution Model

### How Activities Run

1. Orchestration calls `ScheduleTask<T>()` — creates a `TaskScheduled` event in the orchestration history
2. Activity message is placed on the provider-specific work item queue
3. A worker picks up the message and executes the activity (typically as competing consumers)
4. Result is sent back to the orchestration's provider-specific control queue
5. Orchestration replays and sees `TaskCompleted` event in its updated history

### Activity vs Orchestration Context

| Feature | Activity (`TaskContext`) | Orchestration (`OrchestrationContext`) |
| ------- | ------------------------ | -------------------------------------- |
| Instance info | ✅ Available | ✅ Available |
| Schedule tasks | ❌ No | ✅ Yes |
| Create timers | ❌ No | ✅ Yes |
| Wait for events | ❌ No | ✅ Yes |
| Determinism required | ❌ No | ✅ Yes |
| Can call external APIs | ✅ Yes | ❌ Should not |

## Error Handling in Activities

### Throwing Exceptions

Unhandled exceptions fail the activity and become `TaskFailedException` in the orchestration:

```csharp
public class ValidateActivity : TaskActivity<Order, bool>
{
    protected override bool Execute(TaskContext context, Order order)
    {
        if (string.IsNullOrEmpty(order.CustomerId))
        {
            throw new ArgumentException("Customer ID is required");
        }
        return true;
    }
}

// In orchestration
try
{
    await context.ScheduleTask<bool>(typeof(ValidateActivity), order);
}
catch (TaskFailedException ex)
{
    // ex.InnerException contains the original ArgumentException
}
```

### Returning Errors vs Throwing

Consider returning error results for expected failures:

```csharp
public class ProcessOrderActivity : AsyncTaskActivity<Order, OrderResult>
{
    protected override async Task<OrderResult> ExecuteAsync(
        TaskContext context, 
        Order order)
    {
        var inventory = await CheckInventoryAsync(order);
        
        if (!inventory.IsAvailable)
        {
            // Expected case - return result
            return new OrderResult 
            { 
                Success = false, 
                Error = "Insufficient inventory" 
            };
        }
        
        // Unexpected case - throw
        if (order.TotalAmount < 0)
        {
            throw new InvalidOperationException("Invalid order amount");
        }
        
        return new OrderResult { Success = true };
    }
}
```

This approach avoids potentially expensive retries for known failure conditions, and also avoids problems with serializing exceptions.

## Next Steps

- [Orchestrations](orchestrations.md) — Coordinating activities
- [Retries](../features/retries.md) — Configuring automatic retries
- [Error Handling](../features/error-handling.md) — Comprehensive error handling
- [Replay and Durability](replay-and-durability.md) — Understanding the replay model
