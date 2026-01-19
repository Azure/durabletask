# Emulator Provider

The Emulator provider (`LocalOrchestrationService`) is an in-memory implementation for local development and testing. It requires no external dependencies and is ideal for quick iteration.

## Installation

```bash
dotnet add package Microsoft.Azure.DurableTask.Emulator
```

## Usage

```csharp
using DurableTask.Core;
using DurableTask.Emulator;
using Microsoft.Extensions.Logging;

using ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddConsole();
    builder.SetMinimumLevel(LogLevel.Information);
});

// Create in-memory service
var service = new LocalOrchestrationService();

// Create worker and client
var worker = new TaskHubWorker(service, loggerFactory);
var client = new TaskHubClient(service, loggerFactory: loggerFactory);

// ...
```

## Limitations

| Limitation | Description |
| ---------- | ----------- |
| **In-memory only** | All state is lost when process exits |
| **Single process** | Cannot share state across processes |

## Transitioning to Production

When moving from emulator to production, replace `LocalOrchestrationService` with your chosen provider. The rest of your code remains the same:

```csharp
// Development
IOrchestrationService service = new LocalOrchestrationService();

// Production (example: Azure Storage)
IOrchestrationService service = new AzureStorageOrchestrationService(settings);
```

## Next Steps

- [Quickstart](../getting-started/quickstart.md) — Get started with the emulator
- [Choosing a Backend](../getting-started/choosing-a-backend.md) — Select a production provider
