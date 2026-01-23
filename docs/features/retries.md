# Automatic Retries

The Durable Task Framework supports automatic retries for activities and sub-orchestrations. Retries are handled durably - the retry count and timing survive process restarts.

## Basic Retry Configuration

### RetryOptions

Configure retries using `RetryOptions`:

```csharp
var retryOptions = new RetryOptions(
    firstRetryInterval: TimeSpan.FromSeconds(5),
    maxNumberOfAttempts: 3);
```

### Calling with Retry

```csharp
var result = await context.ScheduleWithRetry<string>(
    typeof(UnreliableActivity),
    retryOptions,
    input);
```

## RetryOptions Properties

| Property | Description | Default |
| -------- | ----------- | ------- |
| `FirstRetryInterval` | Delay before the first retry | Required |
| `MaxNumberOfAttempts` | Maximum total attempts (including first) | Required |
| `BackoffCoefficient` | Multiplier for exponential backoff | 1.0 |
| `MaxRetryInterval` | Maximum delay between retries | `TimeSpan.MaxValue` |
| `RetryTimeout` | Total time allowed for all retries | `TimeSpan.MaxValue` |
| `Handle` | Custom exception filter function | Retry all |

## Retry Patterns

### Fixed Interval

Same delay between each retry:

```csharp
var options = new RetryOptions(
    firstRetryInterval: TimeSpan.FromSeconds(10),
    maxNumberOfAttempts: 5);
// BackoffCoefficient defaults to 1.0
// Delays: 10s, 10s, 10s, 10s
```

### Exponential Backoff

Increasing delays between retries:

```csharp
var options = new RetryOptions(
    firstRetryInterval: TimeSpan.FromSeconds(1),
    maxNumberOfAttempts: 5)
{
    BackoffCoefficient = 2.0
};
// Delays: 1s, 2s, 4s, 8s
```

### Exponential with Max Interval

Cap the maximum delay:

```csharp
var options = new RetryOptions(
    firstRetryInterval: TimeSpan.FromSeconds(1),
    maxNumberOfAttempts: 10)
{
    BackoffCoefficient = 2.0,
    MaxRetryInterval = TimeSpan.FromMinutes(1)
};
// Delays: 1s, 2s, 4s, 8s, 16s, 32s, 60s, 60s, 60s
```

### With Timeout

Limit total retry time:

```csharp
var options = new RetryOptions(
    firstRetryInterval: TimeSpan.FromSeconds(5),
    maxNumberOfAttempts: 100)  // High limit
{
    BackoffCoefficient = 2.0,
    MaxRetryInterval = TimeSpan.FromMinutes(5),
    RetryTimeout = TimeSpan.FromHours(1)  // Stop after 1 hour
};
```

## Custom Exception Handling

### Handle Specific Exceptions

```csharp
var options = new RetryOptions(
    firstRetryInterval: TimeSpan.FromSeconds(5),
    maxNumberOfAttempts: 3)
{
    Handle = exception =>
    {
        // Only retry on transient failures
        return exception is HttpRequestException ||
               exception is TimeoutException;
    }
};
```

### Retry Based on Exception Details

```csharp
var options = new RetryOptions(
    firstRetryInterval: TimeSpan.FromSeconds(5),
    maxNumberOfAttempts: 5)
{
    Handle = exception =>
    {
        if (exception is ApiException apiEx)
        {
            // Retry on 429 (rate limit) or 5xx (server errors)
            return apiEx.StatusCode == 429 ||
                   (int)apiEx.StatusCode >= 500;
        }
        return false;
    }
};
```

### Never Retry Specific Errors

```csharp
var options = new RetryOptions(
    firstRetryInterval: TimeSpan.FromSeconds(5),
    maxNumberOfAttempts: 3)
{
    Handle = exception =>
    {
        // Don't retry validation errors
        if (exception is ValidationException)
            return false;
            
        // Don't retry authentication errors
        if (exception is AuthenticationException)
            return false;
            
        return true;  // Retry everything else
    }
};
```

## Sub-Orchestration Retries

Retry sub-orchestrations with the same pattern:

```csharp
var options = new RetryOptions(
    firstRetryInterval: TimeSpan.FromMinutes(1),
    maxNumberOfAttempts: 3);

var result = await context.CreateSubOrchestrationInstanceWithRetry<Result>(
    typeof(ChildOrchestration),
    options,
    input);
```

## Retry Behavior

### What Happens During Retry

1. Activity throws an exception
2. Framework records `TaskFailed` event
3. Retry timer is created (durable)
4. Timer fires, activity is scheduled again
5. If successful, `TaskCompleted` is recorded
6. If failed and attempts remain, go to step 2

### Durability

Retries are durable:

- Retry count survives process restarts
- Timer state is persisted
- No duplicate executions

### Final Failure

After all retries exhausted:

- `TaskFailedException` is thrown in orchestration
- Contains the last exception as `InnerException`
- Orchestration can catch and handle

```csharp
try
{
    var result = await context.ScheduleWithRetry<string>(
        typeof(UnreliableActivity),
        retryOptions,
        input);
}
catch (TaskFailedException ex)
{
    // All retries failed
    _logger.LogError(ex.InnerException, "Activity failed after all retries");
    await context.ScheduleTask<string>(typeof(CompensationActivity), input);
}
```

## Best Practices

### 1. Use Idempotent Activities

Activities may execute multiple times:

```csharp
public class PaymentActivity : AsyncTaskActivity<Payment, PaymentResult>
{
    protected override async Task<PaymentResult> ExecuteAsync(
        TaskContext context, 
        Payment input)
    {
        // Use idempotency key to prevent duplicate charges
        return await _paymentService.ChargeAsync(
            input.Amount,
            idempotencyKey: input.OrderId);
    }
}
```

### 2. Don't Retry Non-Transient Errors

```csharp
var options = new RetryOptions(...)
{
    Handle = ex => !(ex is ValidationException) &&
                   !(ex is NotFoundException)
};
```

### 3. Set Reasonable Timeouts

```csharp
var options = new RetryOptions(
    firstRetryInterval: TimeSpan.FromSeconds(5),
    maxNumberOfAttempts: 10)
{
    RetryTimeout = TimeSpan.FromMinutes(30)  // Don't retry forever
};
```

### 4. Consider Circuit Breaker Pattern

For repeated failures, consider manual circuit breaking:

```csharp
public override async Task<Result> RunTask(OrchestrationContext context, Input input)
{
    int consecutiveFailures = 0;
    
    while (consecutiveFailures < 3)
    {
        try
        {
            return await context.ScheduleWithRetry<Result>(
                typeof(MyActivity),
                retryOptions,
                input);
        }
        catch (TaskFailedException)
        {
            consecutiveFailures++;
            await context.CreateTimer(
                context.CurrentUtcDateTime.AddMinutes(5 * consecutiveFailures),
                true);
        }
    }
    
    throw new Exception("Circuit breaker opened");
}
```

## Comparison with Activity-Level Retry

Activity-level retries (inside the activity code) are not durable and do not survive orchestration restarts. They also do not appear in the orchestration history.

| Feature | Orchestration Retry (ScheduleWithRetry) | Activity-Internal Retry |
| ------- | -------------------------------------- | ---------------------- |
| Durable | ✅ Yes | ❌ No |
| Survives crashes | ✅ Yes | ❌ No |
| Visible in history | ✅ Yes | ❌ No |
| Configurable per-call | ✅ Yes | ⚠️ Limited |

Prefer orchestration-level retries for durability.

## Next Steps

- [Error Handling](error-handling.md) — Comprehensive error handling patterns
- [Timers](timers.md) — Durable timers and delays
- [Activities](../concepts/activities.md) — Writing retry-safe activities
