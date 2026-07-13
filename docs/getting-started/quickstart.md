# Quickstart

This guide walks you through creating your first Durable Task Framework (DTFx) orchestration.

## Overview

In this quickstart, you'll create:
1. An **activity** that performs a simple greeting
2. An **orchestration** that calls the activity
3. A **host** that runs the orchestration

## Step 1: Create a New Project

```bash
dotnet new console -n HelloDurableTask
cd HelloDurableTask
```

## Step 2: Install Packages

For this quickstart, we'll use the in-memory emulator:

```bash
dotnet add package Microsoft.Azure.DurableTask.Core
dotnet add package Microsoft.Azure.DurableTask.Emulator
```

> 💡 For production, see [Choosing a Backend](choosing-a-backend.md) to select an appropriate provider.

## Step 3: Create an Activity

Activities are the basic unit of work in DTFx. Create a file named `GreetActivity.cs`:

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

## Step 4: Create an Orchestration

Orchestrations coordinate activities. Create a file named `GreetingOrchestration.cs`:

```csharp
using DurableTask.Core;

public class GreetingOrchestration : TaskOrchestration<string, string>
{
    public override async Task<string> RunTask(OrchestrationContext context, string input)
    {
        // Call the GreetActivity
        string greeting = await context.ScheduleTask<string>(typeof(GreetActivity), input);
        return greeting;
    }
}
```

## Step 5: Create the Host

Update `Program.cs` to create and run the orchestration:

```csharp
using DurableTask.Core;
using DurableTask.Emulator;
using Microsoft.Extensions.Logging;

// Create logger factory for diagnostics
using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddConsole();
    builder.SetMinimumLevel(LogLevel.Information);
});

// Create the in-memory orchestration service
var service = new LocalOrchestrationService();

// Create and configure the worker
var worker = new TaskHubWorker(service, loggerFactory);
worker.AddTaskOrchestrations(typeof(GreetingOrchestration));
worker.AddTaskActivities(typeof(GreetActivity));

// Start the worker
await worker.StartAsync();
Console.WriteLine("Worker started.");

// Create a client to start orchestrations
var client = new TaskHubClient(service, loggerFactory: loggerFactory);

// Start a new orchestration instance
var instance = await client.CreateOrchestrationInstanceAsync(
    typeof(GreetingOrchestration),
    "World");

Console.WriteLine($"Started orchestration: {instance.InstanceId}");

// Wait for completion
var result = await client.WaitForOrchestrationAsync(
    instance,
    TimeSpan.FromSeconds(30));

Console.WriteLine($"Result: {result.Output}");
Console.WriteLine($"Status: {result.OrchestrationStatus}");

// Stop the worker
await worker.StopAsync();
```

## Step 6: Run the Application

```bash
dotnet run
```

Expected output:
```
Worker started.
Started orchestration: <guid>
Result: "Hello, World!"
Status: Completed
```

## Understanding the Code

### TaskActivity

```csharp
public class GreetActivity : TaskActivity<string, string>
```

- `TaskActivity<TInput, TOutput>` — Base class for activities
- Activities contain the actual work logic
- They are automatically retried on failure (configurable)

### TaskOrchestration

```csharp
public class GreetingOrchestration : TaskOrchestration<string, string>
```

- `TaskOrchestration<TResult, TInput>` — Base class for orchestrations
- Orchestrations coordinate activities and sub-orchestrations
- They must be [deterministic](../concepts/deterministic-constraints.md)

### OrchestrationContext

```csharp
await context.ScheduleTask<string>(typeof(GreetActivity), input);
```

- `OrchestrationContext` provides APIs for scheduling work
- `ScheduleTask` — Schedule an activity
- `CreateSubOrchestrationInstance` — Start a sub-orchestration
- `CreateTimer` — Create a durable timer

### TaskHubWorker and TaskHubClient

- `TaskHubWorker` — Hosts orchestrations and activities
- `TaskHubClient` — Starts and manages orchestration instances

## Next Steps

- [Choosing a Backend](choosing-a-backend.md) — Select a production-ready provider
- [Core Concepts](../concepts/core-concepts.md) — Understand Task Hubs, Workers, and Clients
- [Writing Orchestrations](../concepts/orchestrations.md) — Learn orchestration patterns
- [Writing Activities](../concepts/activities.md) — Learn activity patterns
- [Samples Catalog](../samples/catalog.md) — Explore more examples
