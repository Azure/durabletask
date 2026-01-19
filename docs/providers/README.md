# Backend Providers

The Durable Task Framework supports multiple backend storage providers. Choose based on your requirements for management overhead, throughput, and existing infrastructure.

## Provider Comparison

| Provider | Type | Best For |
| -------- | ---- | -------- |
| [Durable Task Scheduler](durable-task-scheduler.md) ⭐ | Managed | New projects, production workloads |
| [Azure Storage](azure-storage.md) | Self-managed | Existing Azure Storage infrastructure |
| [MSSQL](mssql.md) | Self-managed | SQL Server / Azure SQL infrastructure |
| [Emulator](emulator.md) | In-memory | Local development and testing |
| [Service Fabric](service-fabric.md) | Self-managed | Applications on Service Fabric clusters |
| [Service Bus](service-bus.md) | Self-managed | Legacy (maintenance mode) |
| [Custom Provider](custom-provider.md) | DIY | Specialized storage requirements |

> ⭐ **Recommended:** For new projects, use the [Durable Task Scheduler](durable-task-scheduler.md)—a fully managed Azure service with zero infrastructure management and built-in monitoring.

See [Choosing a Backend](../getting-started/choosing-a-backend.md) for detailed guidance.
