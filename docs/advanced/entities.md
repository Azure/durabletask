# Durable Entities

Durable Entities provide a way to manage small pieces of state with well-defined operations. Entities are addressable by a unique identifier and can be called from orchestrations or signaled from anywhere.

## Entity Support in the Durable Task Framework

> [!IMPORTANT]
> Durable Entities are **not directly supported** for end-user development in the Durable Task Framework. The entity-related APIs that exist in this library (such as `TaskEntity`, `EntityId`, `OrchestrationEntityContext`, etc.) are low-level infrastructure components intended to support [Azure Durable Functions](https://docs.microsoft.com/azure/azure-functions/durable/durable-functions-entities) scenarios.

## Recommended Alternatives

If you want to build applications that leverage the capabilities of Durable Entities, consider one of the following options:

### Azure Durable Functions

[Azure Durable Functions](https://docs.microsoft.com/azure/azure-functions/durable/durable-functions-entities) provides a complete, high-level programming model for Durable Entities with full support for:

- Entity classes and function-based entities
- Calling and signaling entities from orchestrations
- Entity state persistence and management
- Distributed locking and critical sections

### Durable Task SDK with Durable Task Scheduler

The [Durable Task SDK](https://github.com/microsoft/durabletask-dotnet) used together with the [Durable Task Scheduler](durable-task-scheduler.md) provides a modern programming model with entity support. This is the recommended approach for new .NET applications that need durable entity capabilities outside of Azure Functions.

## Next Steps

- [Durable Task Scheduler](../providers/durable-task-scheduler.md) — Learn about the Durable Task Scheduler backend
- [Choosing a Backend](../getting-started/choosing-a-backend.md) — Compare available backend providers
