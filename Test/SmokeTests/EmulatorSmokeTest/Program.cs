// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Reflection;
using DurableTask.Core;
using DurableTask.Emulator;
using Microsoft.Extensions.Logging;

Console.WriteLine("=== DurableTask Emulator Smoke Test ===");
Console.WriteLine();

using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddConsole();
    builder.SetMinimumLevel(LogLevel.Warning);
});

// Print loaded DTFx assembly versions
Console.WriteLine("Loaded DTFx assembly versions:");
foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
{
    string? name = asm.GetName().Name;
    if (name != null && name.StartsWith("DurableTask.", StringComparison.OrdinalIgnoreCase))
    {
        string? infoVersion = asm
            .GetCustomAttribute<System.Reflection.AssemblyInformationalVersionAttribute>()
            ?.InformationalVersion;
        if (infoVersion != null && infoVersion.Contains('+'))
        {
            infoVersion = infoVersion[..infoVersion.IndexOf('+')];
        }
        Console.WriteLine($"  {name} = {infoVersion ?? asm.GetName().Version?.ToString() ?? "unknown"}");
    }
}
Console.WriteLine();

// Create the in-memory orchestration service
var orchestrationService = new LocalOrchestrationService();

// Start the worker with our orchestration and activities
var worker = new TaskHubWorker(orchestrationService, loggerFactory);
await worker
    .AddTaskOrchestrations(typeof(HelloCitiesOrchestration))
    .AddTaskActivities(typeof(SayHelloActivity))
    .StartAsync();

// Create a client and start the orchestration
var client = new TaskHubClient(orchestrationService, loggerFactory: loggerFactory);
OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(
    typeof(HelloCitiesOrchestration), input: null);

Console.WriteLine($"Started orchestration: {instance.InstanceId}");

// Wait for completion
OrchestrationState result = await client.WaitForOrchestrationAsync(
    instance, TimeSpan.FromSeconds(30), CancellationToken.None);

Console.WriteLine($"Orchestration status: {result.OrchestrationStatus}");
Console.WriteLine($"Orchestration output: {result.Output}");

await worker.StopAsync(true);

// Validate result
if (result.OrchestrationStatus != OrchestrationStatus.Completed)
{
    Console.WriteLine("FAIL: Orchestration did not complete successfully.");
    return 1;
}

if (result.Output == null ||
    !result.Output.Contains("Hello, Tokyo!") ||
    !result.Output.Contains("Hello, London!") ||
    !result.Output.Contains("Hello, Seattle!"))
{
    Console.WriteLine("FAIL: Orchestration output did not contain expected greetings.");
    return 1;
}

Console.WriteLine();
Console.WriteLine("PASS: HelloCities orchestration completed with expected output.");
return 0;

// ---- Orchestration ----

/// <summary>
/// A simple function chaining orchestration that calls SayHello for three cities.
/// </summary>
public class HelloCitiesOrchestration : TaskOrchestration<string, object>
{
    public override async Task<string> RunTask(OrchestrationContext context, object input)
    {
        string result = "";
        result += await context.ScheduleTask<string>(typeof(SayHelloActivity), "Tokyo") + " ";
        result += await context.ScheduleTask<string>(typeof(SayHelloActivity), "London") + " ";
        result += await context.ScheduleTask<string>(typeof(SayHelloActivity), "Seattle");
        return result;
    }
}

// ---- Activity ----

/// <summary>
/// A simple activity that returns a greeting for the given city name.
/// </summary>
public class SayHelloActivity : TaskActivity<string, string>
{
    protected override string Execute(TaskContext context, string cityName)
    {
        return $"Hello, {cityName}!";
    }
}
