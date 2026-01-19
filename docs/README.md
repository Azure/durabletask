# Durable Task Framework Documentation

The Durable Task Framework (DTFx) is an open-source framework for writing long-running, fault-tolerant workflow orchestrations in .NET. It provides the foundation for [Azure Durable Functions](https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-overview) and can be used standalone with various backend storage providers.

## Quick Links

| Section | Description |
| ------- | ----------- |
| [Getting Started](getting-started/installation.md) | Installation, quickstart, and choosing a backend |
| [Core Concepts](concepts/core-concepts.md) | Task Hubs, Workers, Clients, and architecture overview |
| [Features](features/retries.md) | Retries, timers, external events, sub-orchestrations, and more |
| [Providers](providers/durable-task-scheduler.md) | Backend storage providers (Durable Task Scheduler, Azure Storage, etc.) |
| [Telemetry](telemetry/distributed-tracing.md) | Distributed tracing, logging, and Application Insights |
| [Advanced Topics](advanced/middleware.md) | Middleware, entities, serialization, and testing |
| [Samples](samples/catalog.md) | Sample projects and code patterns |

## Recommended: Durable Task Scheduler with the modern .NET SDK

For new projects, we recommend using the **[Durable Task Scheduler](providers/durable-task-scheduler.md)**—a fully managed Azure service that provides:

- ✅ A more modern [Durable Task .NET SDK](https://github.com/microsoft/durabletask-dotnet) with improved developer experience
- ✅ Zero infrastructure management
- ✅ Built-in monitoring dashboard
- ✅ Highest throughput of all backends
- ✅ 24/7 Microsoft Azure support with SLA

See [Choosing a Backend](getting-started/choosing-a-backend.md) for a full comparison of all available providers.

## Support

See [Support](support.md) for information about getting help with the Durable Task Framework.
