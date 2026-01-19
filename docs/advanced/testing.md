# Testing Orchestrations

Testing durable orchestrations requires special consideration due to their replay-based execution model. This guide covers strategies and patterns for effectively testing your orchestrations and activities.

## Testing Approaches

There are three main approaches to testing DTFx code:

1. **Unit testing** — Test components in isolation with mocks
2. **Integration testing** — Test with the in-memory emulator
3. **End-to-end testing** — Test with real backend providers

## Unit Testing Activities

Activities are standard async methods, making them straightforward to test:

```csharp
using Microsoft.VisualStudio.TestTools.UnitTesting;

[TestClass]
public class ActivityTests
{
    [TestMethod]
    public async Task ProcessOrderActivity_ValidOrder_ReturnsConfirmation()
    {
        // Arrange
        var activity = new ProcessOrderActivity(
            mockInventoryService.Object,
            mockPaymentService.Object);
        
        var orchestrationInstance = new OrchestrationInstance 
        { 
            InstanceId = "test-123",
            ExecutionId = Guid.NewGuid().ToString()
        };
        var context = new TaskContext(orchestrationInstance);
        var input = new OrderInput { OrderId = "order-1", Amount = 99.99m };
        
        // Act
        var result = await activity.RunAsync(context, input);
        
        // Assert
        Assert.IsNotNull(result);
        Assert.AreEqual("Confirmed", result.Status);
    }
    
    [TestMethod]
    public async Task ProcessOrderActivity_InvalidOrder_ThrowsException()
    {
        // Arrange
        var activity = new ProcessOrderActivity(
            mockInventoryService.Object,
            mockPaymentService.Object);
        
        var orchestrationInstance = new OrchestrationInstance 
        { 
            InstanceId = "test-123",
            ExecutionId = Guid.NewGuid().ToString()
        };
        var context = new TaskContext(orchestrationInstance);
        var input = new OrderInput { OrderId = null };
        
        // Act & Assert
        await Assert.ThrowsExceptionAsync<ArgumentException>(
            () => activity.RunAsync(context, input));
    }
}
```

### Testing with Dependencies

Use dependency injection for testable activities:

```csharp
public class SendEmailActivity : AsyncTaskActivity<EmailRequest, EmailResult>
{
    private readonly IEmailService _emailService;
    
    public SendEmailActivity(IEmailService emailService)
    {
        _emailService = emailService;
    }
    
    protected override async Task<EmailResult> ExecuteAsync(
        TaskContext context, 
        EmailRequest input)
    {
        return await _emailService.SendAsync(input);
    }
}

[TestClass]
public class SendEmailActivityTests
{
    [TestMethod]
    public async Task SendEmail_ValidRequest_Succeeds()
    {
        // Arrange
        var mockEmailService = new Mock<IEmailService>();
        mockEmailService
            .Setup(x => x.SendAsync(It.IsAny<EmailRequest>()))
            .ReturnsAsync(new EmailResult { Success = true, MessageId = "msg-1" });
        
        var activity = new SendEmailActivity(mockEmailService.Object);
        
        // Act
        var result = await activity.ExecuteAsync(
            context: null!,
            input: new EmailRequest { To = "test@example.com", Subject = "Test" });
        
        // Assert
        Assert.IsTrue(result.Success);
        Assert.AreEqual("msg-1", result.MessageId);
        mockEmailService.Verify(x => x.SendAsync(It.IsAny<EmailRequest>()), Times.Once);
    }
}
```

## Unit Testing Orchestrations

Orchestrations are harder to unit test due to their stateful nature and use of the `OrchestrationContext`. The recommended approach is to use **integration testing with the emulator** (see below), but you can also extract testable logic into separate classes.

### Extract Business Logic for Unit Testing

Extract complex business logic into separate, testable classes. Keep orchestration code thin—focused only on coordination:

```csharp
// Testable logic class - no orchestration dependencies
public class OrderLogic : IOrderLogic
{
    public void ValidateOrder(OrderInput input)
    {
        if (string.IsNullOrEmpty(input.OrderId))
            throw new ArgumentException("OrderId is required");
    }
    
    public NextStep DetermineNextStep(InventoryResult inventory)
    {
        return inventory.AllAvailable 
            ? NextStep.ProcessPayment 
            : NextStep.BackOrder;
    }
}

// Unit tests for the extracted logic
[TestClass]
public class OrderLogicTests
{
    [TestMethod]
    public void ValidateOrder_MissingOrderId_ThrowsArgumentException()
    {
        var logic = new OrderLogic();
        var input = new OrderInput { OrderId = null };
        
        Assert.ThrowsException<ArgumentException>(
            () => logic.ValidateOrder(input));
    }
    
    [TestMethod]
    public void DetermineNextStep_AllAvailable_ReturnsProcessPayment()
    {
        var logic = new OrderLogic();
        var inventory = new InventoryResult { AllAvailable = true };
        
        var result = logic.DetermineNextStep(inventory);
        
        Assert.AreEqual(NextStep.ProcessPayment, result);
    }
}
```

Then use the logic in your orchestration:

```csharp
public class OrderOrchestration : TaskOrchestration<OrderResult, OrderInput>
{
    // Use a static/singleton instance or instantiate directly
    // Note: Constructor dependency injection is NOT supported by default
    // because the framework uses Activator.CreateInstance() which requires
    // a parameterless constructor.
    private readonly IOrderLogic _logic = new OrderLogic();
    
    public override async Task<OrderResult> RunTask(
        OrchestrationContext context, 
        OrderInput input)
    {
        // Validate using testable logic
        _logic.ValidateOrder(input);
        
        var inventory = await context.ScheduleTask<InventoryResult>(
            typeof(CheckInventoryActivity), 
            input.Items);
        
        // Process result using testable logic
        var decision = _logic.DetermineNextStep(inventory);
        
        // ... rest of orchestration
    }
}
```

> [!IMPORTANT]
> Orchestrations are instantiated by the framework using `Activator.CreateInstance()`, which requires a parameterless constructor. Constructor-based dependency injection is not supported out of the box. If you need DI, you must implement a custom `ObjectCreator<TaskOrchestration>` and register it with `AddTaskOrchestrations()`.

### Why Not Mock OrchestrationContext?

`OrchestrationContext` is an abstract class with complex internal state management for replay semantics. Creating a proper mock requires implementing many methods and simulating the replay behavior correctly. **Integration testing with the emulator is strongly recommended** instead—it's fast, reliable, and tests the actual orchestration behavior.

## Integration Testing with Emulator

The emulator provides fast, isolated testing without external dependencies:

```csharp
using DurableTask.Core;
using DurableTask.Emulator;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;

[TestClass]
public class OrderOrchestrationIntegrationTests
{
    private ILoggerFactory _loggerFactory;
    private LocalOrchestrationService _service;
    private TaskHubWorker _worker;
    private TaskHubClient _client;
    
    [TestInitialize]
    public async Task Setup()
    {
        _loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
        _service = new LocalOrchestrationService();
        _worker = new TaskHubWorker(_service, _loggerFactory);
        _client = new TaskHubClient(_service, loggerFactory: _loggerFactory);
        
        // Register orchestrations and activities
        _worker.AddTaskOrchestrations(typeof(OrderOrchestration));
        _worker.AddTaskActivities(
            typeof(ValidateOrderActivity),
            typeof(ProcessPaymentActivity),
            typeof(SendConfirmationActivity));
        
        await _worker.StartAsync();
    }
    
    [TestCleanup]
    public async Task Cleanup()
    {
        await _worker.StopAsync(isForced: true);
    }
    
    [TestMethod]
    public async Task OrderOrchestration_ValidOrder_CompletesSuccessfully()
    {
        // Arrange
        var input = new OrderInput
        {
            OrderId = "order-123",
            CustomerId = "customer-456",
            Items = new[] { "item-1", "item-2" }
        };
        
        // Act
        var instance = await _client.CreateOrchestrationInstanceAsync(
            typeof(OrderOrchestration),
            input);
        
        var result = await _client.WaitForOrchestrationAsync(
            instance,
            TimeSpan.FromSeconds(30));
        
        // Assert
        Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
        var output = result.GetOutput<OrderResult>();
        Assert.AreEqual("Confirmed", output.Status);
    }
    
    [TestMethod]
    public async Task OrderOrchestration_InvalidOrder_Fails()
    {
        // Arrange
        var input = new OrderInput { OrderId = null };
        
        // Act
        var instance = await _client.CreateOrchestrationInstanceAsync(
            typeof(OrderOrchestration),
            input);
        
        var result = await _client.WaitForOrchestrationAsync(
            instance,
            TimeSpan.FromSeconds(30));
        
        // Assert
        Assert.AreEqual(OrchestrationStatus.Failed, result.OrchestrationStatus);
    }
}
```

### Testing Timeouts and Timers

```csharp
[TestMethod]
public async Task ReminderOrchestration_SendsReminderAfterDelay()
{
    // Arrange
    var input = new ReminderInput { DelayMinutes = 30 };
    var remindersSent = new List<string>();
    
    // Track activity calls
    _worker.AddTaskActivities(
        new MockSendReminderActivity(reminder => remindersSent.Add(reminder)));
    
    // Act
    var instance = await _client.CreateOrchestrationInstanceAsync(
        typeof(ReminderOrchestration),
        input);
    
    // Note: Emulator runs timers immediately in test mode
    var result = await _client.WaitForOrchestrationAsync(
        instance,
        TimeSpan.FromSeconds(30));
    
    // Assert
    Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
    Assert.AreEqual(1, remindersSent.Count);
}
```

### Testing Sub-Orchestrations

```csharp
[TestMethod]
public async Task ParentOrchestration_CallsChildOrchestration()
{
    // Arrange
    _worker.AddTaskOrchestrations(
        typeof(ParentOrchestration),
        typeof(ChildOrchestration));
    _worker.AddTaskActivities(typeof(ChildActivity));
    
    // Act
    var instance = await _client.CreateOrchestrationInstanceAsync(
        typeof(ParentOrchestration),
        new ParentInput { Value = 5 });
    
    var result = await _client.WaitForOrchestrationAsync(
        instance,
        TimeSpan.FromSeconds(30));
    
    // Assert
    Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
    var output = result.GetOutput<ParentOutput>();
    Assert.AreEqual(10, output.ProcessedValue);  // Child doubled the value
}
```

### Testing External Events

```csharp
[TestMethod]
public async Task ApprovalOrchestration_WaitsForApproval()
{
    // Arrange
    var input = new ApprovalRequest { RequestId = "req-1", Amount = 500 };
    
    var instance = await _client.CreateOrchestrationInstanceAsync(
        typeof(ApprovalOrchestration),
        input);
    
    // Wait a bit for orchestration to reach the wait point
    await Task.Delay(100);
    
    // Act - send approval event
    await _client.RaiseEventAsync(
        instance,
        "ApprovalResult",
        new ApprovalResult { Approved = true, ApprovedBy = "manager@example.com" });
    
    var result = await _client.WaitForOrchestrationAsync(
        instance,
        TimeSpan.FromSeconds(30));
    
    // Assert
    Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
    var output = result.GetOutput<ApprovalOutput>();
    Assert.IsTrue(output.WasApproved);
}
```

## Testing Retry Behavior

```csharp
[TestMethod]
public async Task Orchestration_RetriesFailedActivity()
{
    // Arrange
    var failCount = 0;
    var failingActivity = new Func<TaskContext, string, Task<string>>(
        async (context, input) =>
        {
            failCount++;
            if (failCount < 3)
            {
                throw new TransientException("Temporary failure");
            }
            return "Success";
        });
    
    _worker.AddTaskActivities(
        TestOrchestrationHost.MakeActivity<string, string>(
            "FailingActivity",
            failingActivity));
    
    _worker.AddTaskOrchestrations(typeof(RetryingOrchestration));
    
    // Act
    var instance = await _client.CreateOrchestrationInstanceAsync(
        typeof(RetryingOrchestration),
        "input");
    
    var result = await _client.WaitForOrchestrationAsync(
        instance,
        TimeSpan.FromSeconds(30));
    
    // Assert
    Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
    Assert.AreEqual(3, failCount);  // Failed twice, succeeded on third attempt
}
```

## Testing Replay Behavior

Ensure your orchestrations handle replay correctly:

```csharp
[TestMethod]
public async Task Orchestration_DoesNotDuplicateSideEffects()
{
    // Arrange
    var sideEffectCount = 0;
    
    _worker.AddTaskActivities(
        new CountingSideEffectActivity(() => Interlocked.Increment(ref sideEffectCount)));
    
    _worker.AddTaskOrchestrations(typeof(SideEffectOrchestration));
    
    // Act - run orchestration that will replay
    var instance = await _client.CreateOrchestrationInstanceAsync(
        typeof(SideEffectOrchestration),
        "input");
    
    var result = await _client.WaitForOrchestrationAsync(
        instance,
        TimeSpan.FromSeconds(30));
    
    // Assert - side effect should only occur once despite replays
    Assert.AreEqual(1, sideEffectCount);
}
```

## Test Helpers

### Creating Mock Activities

```csharp
public static class TestHelpers
{
    public static TaskActivity MakeActivity<TInput, TOutput>(
        string name,
        Func<TaskContext, TInput, Task<TOutput>> implementation)
    {
        return new FuncTaskActivity<TInput, TOutput>(implementation)
        {
            Name = name
        };
    }
}

// Usage
var mockActivity = TestHelpers.MakeActivity<OrderInput, OrderResult>(
    "ProcessOrder",
    async (context, input) => new OrderResult { Status = "Confirmed" });
```

### Test Base Class

```csharp
public abstract class OrchestrationTestBase
{
    protected ILoggerFactory LoggerFactory;
    protected LocalOrchestrationService Service;
    protected TaskHubWorker Worker;
    protected TaskHubClient Client;
    
    [TestInitialize]
    public virtual async Task TestInitialize()
    {
        LoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder => builder.AddConsole());
        Service = new LocalOrchestrationService();
        Worker = new TaskHubWorker(Service, LoggerFactory);
        Client = new TaskHubClient(Service, loggerFactory: LoggerFactory);
        
        RegisterOrchestrations(Worker);
        RegisterActivities(Worker);
        
        await Worker.StartAsync();
    }
    
    [TestCleanup]
    public virtual async Task TestCleanup()
    {
        await Worker.StopAsync(isForced: true);
    }
    
    protected abstract void RegisterOrchestrations(TaskHubWorker worker);
    protected abstract void RegisterActivities(TaskHubWorker worker);
    
    protected async Task<TOutput> RunOrchestrationAsync<TOutput>(
        Type orchestrationType,
        object input,
        TimeSpan? timeout = null)
    {
        var instance = await Client.CreateOrchestrationInstanceAsync(
            orchestrationType,
            input);
        
        var result = await Client.WaitForOrchestrationAsync(
            instance,
            timeout ?? TimeSpan.FromSeconds(30));
        
        if (result.OrchestrationStatus == OrchestrationStatus.Failed)
        {
            throw new Exception(
                $"Orchestration failed: {result.FailureDetails?.ErrorMessage}");
        }
        
        return result.GetOutput<TOutput>();
    }
}
```

## Best Practices

### 1. Use the Emulator for Speed

```csharp
// Fast - use emulator for most tests
var service = new LocalOrchestrationService();

// Slow - only for end-to-end tests
var service = new AzureStorageOrchestrationService(settings);
```

### 2. Test Determinism

Verify orchestrations are deterministic:

```csharp
[TestMethod]
public async Task Orchestration_IsDeterministic()
{
    // Run the same orchestration multiple times
    for (int i = 0; i < 5; i++)
    {
        var instance = await _client.CreateOrchestrationInstanceAsync(
            typeof(MyOrchestration),
            new Input { Value = 42 });
        
        var result = await _client.WaitForOrchestrationAsync(
            instance,
            TimeSpan.FromSeconds(30));
        
        Assert.AreEqual(OrchestrationStatus.Completed, result.OrchestrationStatus);
        Assert.AreEqual(84, result.GetOutput<int>());
    }
}
```

### 3. Test Edge Cases

```csharp
[TestMethod]
public async Task Orchestration_HandlesNullInput()
{
    // Test with null
    var instance = await _client.CreateOrchestrationInstanceAsync(
        typeof(MyOrchestration),
        input: null);
    
    var result = await _client.WaitForOrchestrationAsync(
        instance,
        TimeSpan.FromSeconds(30));
    
    // Verify appropriate handling
}

[TestMethod]
public async Task Orchestration_HandlesEmptyList()
{
    var input = new Input { Items = new List<string>() };
    
    var instance = await _client.CreateOrchestrationInstanceAsync(
        typeof(ProcessItemsOrchestration),
        input);
    
    // ...
}
```

### 4. Isolate Tests

```csharp
[TestInitialize]
public async Task Setup()
{
    // Create fresh service for each test
    _service = new LocalOrchestrationService();
    // ...
}
```

## Sample Test Project

See the complete test examples:
- [DurableTask.Samples.Tests](../../Test/DurableTask.Samples.Tests)
- [DurableTask.Core.Tests](../../Test/DurableTask.Core.Tests)
- [DurableTask.AzureStorage.Tests](../../Test/DurableTask.AzureStorage.Tests)

## Next Steps

- [Middleware](middleware.md) — Custom middleware
- [Serialization](serialization.md) — Custom serialization
- [Error Handling](../features/error-handling.md) — Exception handling
