# External Events

External events allow orchestrations to receive data from outside sources. This enables human interaction patterns, webhooks, and inter-orchestration communication.

## The Event Pattern

DTFx uses the `OnEvent` method override combined with `TaskCompletionSource` to handle external events. This pattern provides full control over event handling and is the standard approach in the framework.

> [!NOTE]
> If you're familiar with [Azure Durable Functions](https://learn.microsoft.com/azure/azure-functions/durable/), note that DTFx does not have a built-in `WaitForExternalEvent<T>()` helper method. Instead, DTFx provides the lower-level `OnEvent` pattern shown below, which Durable Functions builds upon.

This pattern:

1. Creates a `TaskCompletionSource<T>` to represent the pending event
2. Awaits the `TaskCompletionSource.Task` in `RunTask`
3. Overrides `OnEvent` to receive events and complete the task

### Basic Event Wait

```csharp
public class SignalOrchestration : TaskOrchestration<string, string>
{
    TaskCompletionSource<string> resumeHandle;

    public override async Task<string> RunTask(OrchestrationContext context, string input)
    {
        // Wait for external signal
        string user = await WaitForSignal();
        
        // Continue with the workflow
        string greeting = await context.ScheduleTask<string>(typeof(SendGreetingTask), user);
        return greeting;
    }

    async Task<string> WaitForSignal()
    {
        this.resumeHandle = new TaskCompletionSource<string>();
        string data = await this.resumeHandle.Task;
        this.resumeHandle = null;
        return data;
    }

    public override void OnEvent(OrchestrationContext context, string name, string input)
    {
        // Complete the pending task when event arrives
        this.resumeHandle?.SetResult(input);
    }
}
```

### Typed Event Data

For strongly-typed event data, deserialize in `OnEvent`:

```csharp
public class ApprovalOrchestration : TaskOrchestration<ApprovalResult, ApprovalRequest>
{
    TaskCompletionSource<ApprovalResponse> approvalHandle;

    public override async Task<ApprovalResult> RunTask(
        OrchestrationContext context, 
        ApprovalRequest request)
    {
        // Send approval request
        await context.ScheduleTask<bool>(typeof(SendApprovalEmailActivity), request);
        
        // Wait for approval response
        this.approvalHandle = new TaskCompletionSource<ApprovalResponse>();
        var response = await this.approvalHandle.Task;
        this.approvalHandle = null;
        
        return new ApprovalResult 
        { 
            IsApproved = response.IsApproved,
            ApprovedBy = response.ApprovedBy
        };
    }

    public override void OnEvent(OrchestrationContext context, string name, string input)
    {
        if (name == "Approval" && this.approvalHandle != null)
        {
            var response = context.MessageDataConverter.Deserialize<ApprovalResponse>(input);
            this.approvalHandle.SetResult(response);
        }
    }
}
```

### Wait with Timeout

Combine with timers to implement timeouts:

```csharp
public class TimedApprovalOrchestration : TaskOrchestration<Result, Request>
{
    TaskCompletionSource<string> eventHandle;

    public override async Task<Result> RunTask(OrchestrationContext context, Request input)
    {
        // Set up the event wait
        this.eventHandle = new TaskCompletionSource<string>();
        var eventTask = this.eventHandle.Task;
        
        // Set up timeout
        using var cts = new CancellationTokenSource();
        var timeoutTask = context.CreateTimer(
            context.CurrentUtcDateTime.AddHours(24),
            "timeout",
            cts.Token);
        
        // Wait for either event or timeout
        var winner = await Task.WhenAny(eventTask, timeoutTask);
        
        if (winner == eventTask)
        {
            cts.Cancel();
            var response = await eventTask;
            this.eventHandle = null;
            return new Result { Response = response, TimedOut = false };
        }
        else
        {
            this.eventHandle = null;
            return new Result { TimedOut = true };
        }
    }

    public override void OnEvent(OrchestrationContext context, string name, string input)
    {
        if (name == "UserResponse")
        {
            this.eventHandle?.SetResult(input);
        }
    }
}
```

### Multiple Event Types

Handle different event types with named checks:

```csharp
public class MultiEventOrchestration : TaskOrchestration<Result, Request>
{
    TaskCompletionSource<(string EventType, string Data)> eventHandle;

    public override async Task<Result> RunTask(OrchestrationContext context, Request input)
    {
        this.eventHandle = new TaskCompletionSource<(string, string)>();
        
        using var cts = new CancellationTokenSource();
        var eventTask = this.eventHandle.Task;
        var timeoutTask = context.CreateTimer(
            context.CurrentUtcDateTime.AddDays(7),
            "timeout",
            cts.Token);
        
        var winner = await Task.WhenAny(eventTask, timeoutTask);
        cts.Cancel();
        this.eventHandle = null;
        
        if (winner == timeoutTask)
        {
            return new Result { Status = "TimedOut" };
        }
        
        var (eventType, data) = await eventTask;
        
        return eventType switch
        {
            "Approve" => new Result { Status = "Approved" },
            "Reject" => new Result { Status = "Rejected", Reason = data },
            "Cancel" => new Result { Status = "Cancelled" },
            _ => new Result { Status = "Unknown" }
        };
    }

    public override void OnEvent(OrchestrationContext context, string name, string input)
    {
        if (this.eventHandle != null && 
            (name == "Approve" || name == "Reject" || name == "Cancel"))
        {
            this.eventHandle.SetResult((name, input));
        }
    }
}
```

## Sending Events

### From TaskHubClient

```csharp
var service = GetOrchestrationService();
var client = new TaskHubClient(service, loggerFactory: loggerFactory);

// Send event to a specific orchestration instance
await client.RaiseEventAsync(
    instance,                           // OrchestrationInstance
    eventName: "Approval",              // Event name (passed to OnEvent)
    eventData: new ApprovalData         // Event payload (serialized to string)
    {
        IsApproved = true,
        ApprovedBy = "manager@company.com"
    });

// Using instance ID directly
await client.RaiseEventAsync(
    new OrchestrationInstance { InstanceId = "order-12345" },
    "Approval",
    new ApprovalData { IsApproved = true });
```

### From Another Orchestration

Orchestrations cannot directly raise events. Use an activity:

```csharp
public override async Task RunTask(OrchestrationContext context, SignalInput input)
{
    // Do some work...
    
    // Use an activity to send the event
    await context.ScheduleTask<bool>(typeof(SendEventActivity), new SendEventInput
    {
        TargetInstanceId = input.TargetOrchestrationId,
        EventName = "DataReady",
        EventData = input.Data
    });
}

// Activity to send the event
public class SendEventActivity : AsyncTaskActivity<SendEventInput, bool>
{
    private readonly TaskHubClient _client;
    
    public SendEventActivity(TaskHubClient client)
    {
        _client = client;
    }
    
    protected override async Task<bool> ExecuteAsync(
        TaskContext context, 
        SendEventInput input)
    {
        await _client.RaiseEventAsync(
            new OrchestrationInstance { InstanceId = input.TargetInstanceId },
            input.EventName,
            input.EventData);
        return true;
    }
}
```

### From External Systems (Webhooks)

```csharp
// In an ASP.NET Core controller
[ApiController]
[Route("api/[controller]")]
public class WebhookController : ControllerBase
{
    private readonly TaskHubClient _client;
    
    public WebhookController(TaskHubClient client)
    {
        _client = client;
    }
    
    [HttpPost("approve/{instanceId}")]
    public async Task<IActionResult> Approve(
        string instanceId, 
        [FromBody] ApprovalRequest request)
    {
        await _client.RaiseEventAsync(
            new OrchestrationInstance { InstanceId = instanceId },
            "Approval",
            new ApprovalData 
            { 
                IsApproved = request.Approved,
                ApprovedBy = User.Identity?.Name
            });
        
        return Ok();
    }
}
```

## Event Patterns

### Human Approval Workflow

```csharp
public class ApprovalWorkflow : TaskOrchestration<ApprovalResult, ApprovalRequest>
{
    TaskCompletionSource<ApprovalResponse> approvalHandle;

    public override async Task<ApprovalResult> RunTask(
        OrchestrationContext context, 
        ApprovalRequest request)
    {
        // Step 1: Send approval request email
        await context.ScheduleTask<bool>(typeof(SendApprovalEmailActivity), new EmailData
        {
            To = request.ApproverEmail,
            Subject = $"Approval needed: {request.Title}",
            ApprovalUrl = $"https://myapp.com/approve/{context.OrchestrationInstance.InstanceId}"
        });
        
        // Step 2: Wait for response with 7-day timeout
        this.approvalHandle = new TaskCompletionSource<ApprovalResponse>();
        
        using var cts = new CancellationTokenSource();
        var approvalTask = this.approvalHandle.Task;
        var timeoutTask = context.CreateTimer(
            context.CurrentUtcDateTime.AddDays(7),
            "timeout",
            cts.Token);
        
        var winner = await Task.WhenAny(approvalTask, timeoutTask);
        cts.Cancel();
        
        if (winner == timeoutTask)
        {
            this.approvalHandle = null;
            await context.ScheduleTask<bool>(typeof(SendTimeoutNotificationActivity), request);
            return new ApprovalResult { Status = ApprovalStatus.TimedOut };
        }
        
        var response = await approvalTask;
        this.approvalHandle = null;
        
        // Step 3: Process the decision
        if (response.IsApproved)
        {
            await context.ScheduleTask<bool>(typeof(ProcessApprovalActivity), request);
            return new ApprovalResult { Status = ApprovalStatus.Approved };
        }
        else
        {
            await context.ScheduleTask<bool>(typeof(ProcessRejectionActivity), new RejectionData
            {
                Request = request,
                Reason = response.RejectionReason
            });
            return new ApprovalResult 
            { 
                Status = ApprovalStatus.Rejected,
                Reason = response.RejectionReason
            };
        }
    }

    public override void OnEvent(OrchestrationContext context, string name, string input)
    {
        if (name == "ApprovalResponse" && this.approvalHandle != null)
        {
            var response = context.MessageDataConverter.Deserialize<ApprovalResponse>(input);
            this.approvalHandle.SetResult(response);
        }
    }
}
```

### Sequential Multi-Step Events

```csharp
public class MultiStepOrchestration : TaskOrchestration<Result, Request>
{
    TaskCompletionSource<string> currentEventHandle;
    string currentEventName;

    public override async Task<Result> RunTask(OrchestrationContext context, Request input)
    {
        // Wait for step 1
        var step1 = await WaitForEvent("Step1Complete");
        await context.ScheduleTask<bool>(typeof(ProcessStep1Activity), step1);
        
        // Wait for step 2
        var step2 = await WaitForEvent("Step2Complete");
        await context.ScheduleTask<bool>(typeof(ProcessStep2Activity), step2);
        
        // Wait for step 3
        var step3 = await WaitForEvent("Step3Complete");
        await context.ScheduleTask<bool>(typeof(ProcessStep3Activity), step3);
        
        return new Result { Success = true };
    }

    async Task<string> WaitForEvent(string eventName)
    {
        this.currentEventName = eventName;
        this.currentEventHandle = new TaskCompletionSource<string>();
        var result = await this.currentEventHandle.Task;
        this.currentEventHandle = null;
        this.currentEventName = null;
        return result;
    }

    public override void OnEvent(OrchestrationContext context, string name, string input)
    {
        if (name == this.currentEventName && this.currentEventHandle != null)
        {
            this.currentEventHandle.SetResult(input);
        }
    }
}
```

## Event Behavior

### Event Buffering

Events sent before the orchestration reaches its wait point are **buffered** and delivered when `OnEvent` is called during replay. The framework replays the event from history.

### Event History

Events are recorded in the orchestration history:

```text
EventRaised { Name: "Approval", Input: "{...}" }
```

During replay, the `OnEvent` method is called with the same event data from history, one at a time using a single thread, ensuring deterministic behavior. The thread used to call `OnEvent` is the same thread that runs the orchestration code.

## Best Practices

### 1. Use Meaningful Event Names

```csharp
public override void OnEvent(OrchestrationContext context, string name, string input)
{
    // ✅ Good - clear, descriptive names
    if (name == "OrderApproved") { ... }
    if (name == "PaymentReceived") { ... }
    
    // ❌ Bad - unclear names
    if (name == "Event1") { ... }
    if (name == "Data") { ... }
}
```

### 2. Include Timeout

```csharp
// ✅ Good - has timeout
var eventTask = this.eventHandle.Task;
var timeoutTask = context.CreateTimer(deadline, "timeout", cts.Token);
await Task.WhenAny(eventTask, timeoutTask);

// ⚠️ Risky - waits forever
await this.eventHandle.Task;
```

### 3. Clean Up Handles

```csharp
public override async Task<Result> RunTask(OrchestrationContext context, Request input)
{
    this.eventHandle = new TaskCompletionSource<string>();
    
    try
    {
        var result = await this.eventHandle.Task;
        return new Result { Data = result };
    }
    finally
    {
        // ✅ Always clean up
        this.eventHandle = null;
    }
}
```

### 4. Validate Event Data

```csharp
public override void OnEvent(OrchestrationContext context, string name, string input)
{
    if (name == "Approval" && this.approvalHandle != null)
    {
        var response = context.MessageDataConverter.Deserialize<ApprovalResponse>(input);
        
        // Validate before completing
        if (string.IsNullOrEmpty(response.ApprovedBy))
        {
            // Could log warning or ignore invalid event
            return;
        }
        
        this.approvalHandle.SetResult(response);
    }
}
```

### 5. Document Expected Events

```csharp
/// <summary>
/// Order processing orchestration.
/// 
/// Expected external events:
/// - "PaymentConfirmed" (PaymentData): Payment has been processed
/// - "ShippingReady" (ShippingData): Order is ready to ship
/// - "Cancel" (CancellationData): Cancel the order
/// </summary>
public class OrderOrchestration : TaskOrchestration<OrderResult, OrderInput>
{
    // ...
}
```

## Next Steps

- [Timers](timers.md) — Combining events with timeouts
- [Sub-Orchestrations](sub-orchestrations.md) — Coordinating child workflows
- [Error Handling](error-handling.md) — Handling event failures
