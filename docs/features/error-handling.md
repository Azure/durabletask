# Error Handling

The Durable Task Framework provides robust error handling capabilities for orchestrations and activities. This guide covers exception handling, compensation, and recovery patterns.

## Activity Exceptions

### Basic Exception Handling

When an activity throws an exception, it becomes a `TaskFailedException` in the orchestration:

```csharp
public override async Task<Result> RunTask(OrchestrationContext context, Input input)
{
    try
    {
        var result = await context.ScheduleTask<string>(typeof(RiskyActivity), input);
        return new Result { Success = true, Data = result };
    }
    catch (TaskFailedException ex)
    {
        // ex.InnerException contains the original exception
        return new Result { Success = false, Error = ex.InnerException?.Message };
    }
}
```

### Exception Details

```csharp
catch (TaskFailedException ex)
{
    var originalException = ex.InnerException;
    var activityName = ex.Name;           // "RiskyActivity"
    var scheduledEventId = ex.ScheduleId; // Event ID in history
    
    _logger.LogError(originalException, 
        "Activity {Activity} failed", activityName);
}
```

## Error Propagation Modes

The `TaskHubWorker.ErrorPropagationMode` property controls how exception information is propagated from failed activities and sub-orchestrations.

### SerializeExceptions (Default)

The default mode serializes the original exception and makes it available via `InnerException`:

```csharp
worker.ErrorPropagationMode = ErrorPropagationMode.SerializeExceptions;

// In orchestration:
catch (TaskFailedException ex)
{
    // Original exception is deserialized and available
    var originalException = ex.InnerException;
    
    if (originalException is InvalidOperationException invalidOp)
    {
        // Can catch specific exception types
    }
}
```

**Limitations:**

- Not all exception types can be serialized/deserialized correctly
- Custom exceptions may lose data if not properly serializable
- Doesn't work across language boundaries (e.g., polyglot scenarios)

### UseFailureDetails (Recommended)

The `UseFailureDetails` mode provides consistent, structured error information via `FailureDetails`:

```csharp
worker.ErrorPropagationMode = ErrorPropagationMode.UseFailureDetails;
```

With this mode:

- `InnerException` is **always null**
- Error details are available via `FailureDetails` property
- Works consistently across all exception types and language runtimes

```csharp
catch (TaskFailedException ex)
{
    // InnerException is null in UseFailureDetails mode
    // Use FailureDetails instead
    FailureDetails details = ex.FailureDetails;
    
    string errorType = details.ErrorType;      // e.g., "System.InvalidOperationException"
    string errorMessage = details.ErrorMessage; // The exception message
    string stackTrace = details.StackTrace;     // Full stack trace
    bool isNonRetriable = details.IsNonRetriable;
    
    // Check for inner failures (nested exceptions)
    FailureDetails innerFailure = details.InnerFailure;
}
```

#### Checking Exception Types

Use `IsCausedBy<T>()` to check exception types without deserializing:

```csharp
catch (TaskFailedException ex) when (ex.FailureDetails?.IsCausedBy<InvalidOperationException>() == true)
{
    // Handle InvalidOperationException
}
catch (TaskFailedException ex) when (ex.FailureDetails?.IsCausedBy<TimeoutException>() == true)
{
    // Handle TimeoutException
}
```

#### Sub-Orchestration Failures

The same pattern applies to `SubOrchestrationFailedException`:

```csharp
try
{
    await context.CreateSubOrchestrationInstance<Result>(
        typeof(ChildOrchestration),
        input);
}
catch (SubOrchestrationFailedException ex)
{
    FailureDetails details = ex.FailureDetails;
    
    _logger.LogError(
        "Child orchestration failed: {ErrorType}: {Message}",
        details.ErrorType,
        details.ErrorMessage);
}
```

#### When to Use UseFailureDetails

Use `UseFailureDetails` when:

- You need consistent error handling across all exception types
- Running orchestrations/activities out-of-process or in other language runtimes
- Custom exceptions may not serialize correctly
- You want to avoid deserialization issues with `InnerException`

> [!WARNING]
> Changing `ErrorPropagationMode` on an existing deployment can break in-flight orchestrations if they contain exception handling logic that depends on `InnerException`. Plan changes carefully and consider using [versioning strategies](versioning.md).

#### Custom Exception Properties

When using `UseFailureDetails`, you can include custom properties from your exceptions in the `FailureDetails.Properties` dictionary by implementing `IExceptionPropertiesProvider`:

```csharp
public class CustomExceptionPropertiesProvider : IExceptionPropertiesProvider
{
    public IDictionary<string, object?>? GetExceptionProperties(Exception exception)
    {
        // Extract custom properties from known exception types
        if (exception is OrderProcessingException orderEx)
        {
            return new Dictionary<string, object?>
            {
                ["OrderId"] = orderEx.OrderId,
                ["FailureStage"] = orderEx.Stage,
                ["RetryCount"] = orderEx.RetryCount
            };
        }
        
        if (exception is ValidationException validationEx)
        {
            return new Dictionary<string, object?>
            {
                ["FieldName"] = validationEx.FieldName,
                ["ValidationRule"] = validationEx.Rule
            };
        }
        
        // Return null for exceptions without custom properties
        return null;
    }
}
```

Register the provider with the `TaskHubWorker`:

```csharp
var worker = new TaskHubWorker(orchestrationService, loggerFactory);
worker.ErrorPropagationMode = ErrorPropagationMode.UseFailureDetails;
worker.ExceptionPropertiesProvider = new CustomExceptionPropertiesProvider();
```

Access the custom properties in your orchestration's error handling:

```csharp
catch (TaskFailedException ex)
{
    FailureDetails details = ex.FailureDetails;
    
    if (details.Properties != null)
    {
        if (details.Properties.TryGetValue("OrderId", out var orderId))
        {
            _logger.LogError("Order {OrderId} failed: {Message}", orderId, details.ErrorMessage);
        }
        
        if (details.Properties.TryGetValue("RetryCount", out var retryCount) && 
            retryCount is int count && count >= 3)
        {
            // Too many retries, escalate
            await context.ScheduleTask<bool>(typeof(EscalateFailureActivity), details);
        }
    }
}
```

> [!NOTE]
> Property values should be simple, serializable types (strings, numbers, booleans). Complex objects may not serialize correctly across process boundaries.

### Handling Specific Exception Types

```csharp
try
{
    await context.ScheduleTask<string>(typeof(PaymentActivity), payment);
}
catch (TaskFailedException ex) when (ex.InnerException is InsufficientFundsException)
{
    // Handle specific business error
    await context.ScheduleTask<bool>(typeof(NotifyCustomerActivity), 
        "Payment failed: Insufficient funds");
}
catch (TaskFailedException ex) when (ex.InnerException is PaymentGatewayException)
{
    // Retry with different gateway
    await context.ScheduleTask<string>(typeof(BackupPaymentActivity), payment);
}
catch (TaskFailedException)
{
    // Handle all other failures
    throw;
}
```

## Automatic Retries

Use `ScheduleWithRetry` for transient failures:

```csharp
var retryOptions = new RetryOptions(
    firstRetryInterval: TimeSpan.FromSeconds(5),
    maxNumberOfAttempts: 3)
{
    BackoffCoefficient = 2.0,
    Handle = ex => ex is TimeoutException || ex is HttpRequestException
};

try
{
    await context.ScheduleWithRetry<string>(
        typeof(UnreliableActivity),
        retryOptions,
        input);
}
catch (TaskFailedException ex)
{
    // All retries exhausted
}
```

See [Retries](retries.md) for detailed retry configuration.

## Sub-Orchestration Exceptions

```csharp
try
{
    await context.CreateSubOrchestrationInstance<Result>(
        typeof(ChildOrchestration),
        input);
}
catch (SubOrchestrationFailedException ex)
{
    // Child orchestration threw an unhandled exception
    var failureReason = ex.InnerException?.Message;
}
```

## Compensation Patterns

### Saga Pattern

Compensate previous steps when a later step fails:

```csharp
public override async Task<OrderResult> RunTask(
    OrchestrationContext context, 
    OrderInput input)
{
    // Track completed steps for compensation
    var completedSteps = new List<string>();
    
    try
    {
        // Step 1: Reserve inventory
        await context.ScheduleTask<bool>(typeof(ReserveInventoryActivity), input);
        completedSteps.Add("inventory");
        
        // Step 2: Charge payment
        await context.ScheduleTask<bool>(typeof(ChargePaymentActivity), input);
        completedSteps.Add("payment");
        
        // Step 3: Ship order (might fail)
        await context.ScheduleTask<bool>(typeof(ShipOrderActivity), input);
        
        return new OrderResult { Success = true };
    }
    catch (TaskFailedException ex)
    {
        // Compensate in reverse order
        if (completedSteps.Contains("payment"))
        {
            await context.ScheduleTask<bool>(typeof(RefundPaymentActivity), input);
        }
        
        if (completedSteps.Contains("inventory"))
        {
            await context.ScheduleTask<bool>(typeof(ReleaseInventoryActivity), input);
        }
        
        return new OrderResult 
        { 
            Success = false, 
            Error = ex.InnerException?.Message 
        };
    }
}
```

### Compensation with Sub-Orchestrations

```csharp
public override async Task<Result> RunTask(OrchestrationContext context, Input input)
{
    try
    {
        await context.CreateSubOrchestrationInstance<Result>(
            typeof(ProcessOrderOrchestration),
            input);
    }
    catch (SubOrchestrationFailedException)
    {
        // Run compensation orchestration
        await context.CreateSubOrchestrationInstance<CompensationResult>(
            typeof(CompensateOrderOrchestration),
            new CompensationInput { OriginalInput = input });
    }
}
```

## Error Result Pattern

Return errors as results instead of throwing:

```csharp
// Activity returns result with error info
public class ProcessOrderActivity : AsyncTaskActivity<Order, OrderResult>
{
    protected override async Task<OrderResult> ExecuteAsync(
        TaskContext context, 
        Order order)
    {
        if (!await ValidateOrderAsync(order))
        {
            // Return error instead of throwing
            return new OrderResult 
            { 
                Success = false, 
                ErrorCode = "VALIDATION_FAILED",
                ErrorMessage = "Order validation failed"
            };
        }
        
        // Process order...
        return new OrderResult { Success = true, OrderId = newOrderId };
    }
}

// Orchestration checks result
public override async Task<Result> RunTask(OrchestrationContext context, Order input)
{
    var result = await context.ScheduleTask<OrderResult>(typeof(ProcessOrderActivity), input);
    
    if (!result.Success)
    {
        // Handle error without exception
        await context.ScheduleTask<bool>(typeof(NotifyErrorActivity), result);
        return new Result { Success = false, Error = result.ErrorMessage };
    }
    
    return new Result { Success = true, OrderId = result.OrderId };
}
```

## Timeout Handling

### Activity Timeout

Activities don't have built-in timeout. Handle in orchestration:

```csharp
public override async Task<Result> RunTask(OrchestrationContext context, Input input)
{
    using var cts = new CancellationTokenSource();
    
    var activityTask = context.ScheduleTask<string>(typeof(LongRunningActivity), input);
    var timeoutTask = context.CreateTimer(
        context.CurrentUtcDateTime.AddMinutes(30),
        true,
        cts.Token);
    
    var winner = await Task.WhenAny(activityTask, timeoutTask);
    
    if (winner == activityTask)
    {
        cts.Cancel();
        return new Result { Success = true, Data = await activityTask };
    }
    else
    {
        // Activity is still running but we've timed out
        // Note: Activity will complete eventually, but result is ignored
        return new Result { Success = false, TimedOut = true };
    }
}
```

### Orchestration-Level Timeout

```csharp
public override async Task<Result> RunTask(OrchestrationContext context, Input input)
{
    var deadline = context.CurrentUtcDateTime.AddHours(4);
    
    while (context.CurrentUtcDateTime < deadline)
    {
        var status = await context.ScheduleTask<Status>(typeof(CheckStatusActivity), input);
        
        if (status.IsComplete)
            return new Result { Success = true };
        
        await context.CreateTimer(context.CurrentUtcDateTime.AddMinutes(5), true);
    }
    
    return new Result { Success = false, TimedOut = true };
}
```

## Circuit Breaker Pattern

Prevent repeated failures:

```csharp
public override async Task<Result> RunTask(OrchestrationContext context, State state)
{
    state ??= new State();
    
    // Circuit breaker check
    if (state.ConsecutiveFailures >= 5)
    {
        var cooldownEnd = state.LastFailure.AddMinutes(15);
        if (context.CurrentUtcDateTime < cooldownEnd)
        {
            // Circuit is open - wait before retry
            await context.CreateTimer(cooldownEnd, true);
        }
        state.ConsecutiveFailures = 0;  // Reset after cooldown
    }
    
    try
    {
        var result = await context.ScheduleTask<string>(typeof(ExternalServiceActivity), state.Input);
        state.ConsecutiveFailures = 0;
        return new Result { Success = true, Data = result };
    }
    catch (TaskFailedException)
    {
        state.ConsecutiveFailures++;
        state.LastFailure = context.CurrentUtcDateTime;
        
        // Continue to retry with backoff
        context.ContinueAsNew(state);
        return null;
    }
}
```

## Best Practices Summary

| Practice | Description |
| -------- | ----------- |
| **Use `UseFailureDetails` mode** | Prefer `ErrorPropagationMode.UseFailureDetails` for consistent error handling |
| **Use retries for transient failures** | Configure `RetryOptions` for HTTP, timeout errors |
| **Return errors for expected failures** | Use result types instead of exceptions for business errors |
| **Implement compensation** | Use Saga pattern for multi-step transactions |
| **Set timeouts** | Don't let orchestrations wait indefinitely |
| **Log with context** | Include instance ID, activity name, error details |
| **Test failure scenarios** | Verify compensation and recovery logic |

## Next Steps

- [Retries](retries.md) — Configuring automatic retries
- [Replay and Durability](../concepts/replay-and-durability.md) — Understanding exception persistence
- [Testing](../advanced/testing.md) — Testing error handling
