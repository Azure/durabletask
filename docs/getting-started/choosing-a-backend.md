# Choosing a Backend

The Durable Task Framework (DTFx) supports multiple backend storage providers. This guide helps you choose the right one for your needs.

## Recommendation: Durable Task Scheduler

For most new projects, we recommend the **[Durable Task Scheduler](../providers/durable-task-scheduler.md)**—a fully managed Azure service that eliminates infrastructure management and provides the best developer experience.

## Provider Comparison

| Feature | [Durable Task Scheduler](../providers/durable-task-scheduler.md) | [Azure Storage](../providers/azure-storage.md) | [MSSQL](../providers/mssql.md) | [Service Bus](../providers/service-bus.md) | [Service Fabric](../providers/service-fabric.md) | [Emulator](../providers/emulator.md) |
| ------- | ---------------------- | ------------- | ----- | ----------- | -------------- | -------- |
| **Type** | ⭐ Managed service | Self-managed | Self-managed | Self-managed | Self-managed | In-memory |
| **Production ready** | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes | ❌ No |
| **Azure support SLA** | ✅ Yes | ❌ No | ❌ No | ❌ No | ❌ No | ❌ No |
| **Infrastructure** | None required | Storage account | SQL Server database | Service Bus namespace | Service Fabric cluster | None |
| **Throughput** | Very high | Moderate+ | Moderate+ | Moderate | Unknown | N/A |
| **Latency** | Low | Moderate | Low | Moderate+ | Unknown | Very low |
| **Built-in dashboard** | ✅ Yes | ❌ No | ❌ No | ❌ No | ❌ No | ❌ No |
| **Managed identity** | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes | N/A | N/A |
| **Local emulator** | ✅ Docker | N/A | ✅ SQL Server | N/A | N/A | ✅ Built-in |
| **Cost model** | Fixed monthly or per-operation | Storage transactions | Database DTUs/vCores | Messaging units | Cluster nodes | Free |

## When to Use Each Provider

### Durable Task Scheduler ⭐ Recommended

**Best for:**

- ✅ New projects and greenfield development
- ✅ Production workloads requiring enterprise support
- ✅ Teams that want to minimize operational overhead
- ✅ High-throughput scenarios
- ✅ Applications needing built-in monitoring

**Considerations:**

- Requires Azure subscription
- Cost based on operations (see [pricing](https://learn.microsoft.com/azure/azure-functions/durable/durable-task-scheduler/durable-task-scheduler-dedicated-sku))

👉 **[Get started with Durable Task Scheduler](../providers/durable-task-scheduler.md)**

---

### Azure Storage

**Best for:**

- ✅ Existing Azure Storage deployments
- ✅ Cost-sensitive workloads with moderate throughput
- ✅ Scenarios requiring data residency control
- ✅ Teams already managing Azure Storage infrastructure

**Considerations:**

- Throughput limited by Azure Storage transaction limits
- Requires management of storage account, queues, tables, and blobs
- No built-in monitoring dashboard

👉 **[Get started with Azure Storage](../providers/azure-storage.md)**

---

### MSSQL (Microsoft SQL Server)

**Best for:**

- ✅ Non-Azure or hybrid deployments
- ✅ Teams with existing SQL Server expertise
- ✅ Scenarios requiring direct database queries against orchestration state
- ✅ Environments with strict BCDR requirements

**Considerations:**

- Requires management of SQL Server database
- State is stored in indexed tables with stored procedures for direct querying
- Available for Azure SQL Database, SQL Server, or any compatible MSSQL database

👉 **[Get started with MSSQL](https://github.com/microsoft/durabletask-mssql)**

---

### Service Bus

**Best for:**

- ✅ Existing Service Bus deployments
- ✅ Low(er)-latency message delivery requirements

**Considerations:**

- Requires management of Service Bus namespace
- Tracking store requires separate Azure Storage account

👉 **[Get started with Service Bus](../providers/service-bus.md)**

---

### Service Fabric

**Best for:**

- ✅ Existing Service Fabric clusters
- ✅ Integration with Service Fabric stateful services

**Considerations:**

- Requires Service Fabric cluster management
- Tightly coupled to Service Fabric ecosystem

👉 **[Get started with Service Fabric](../providers/service-fabric.md)**

---

### Emulator

**Best for:**

- ✅ Local development and testing
- ✅ Unit tests and integration tests
- ✅ Learning and experimentation

**Considerations:**

- In-memory only—data is lost on restart
- Not suitable for production use
- Single-process only

👉 **[Get started with Emulator](../providers/emulator.md)**

---

### Netherite ⚠️ Deprecated

> **Warning:** Netherite is being deprecated and is not recommended for new projects.

Netherite is an ultra-high performance backend developed by Microsoft Research that uses Azure Event Hubs and Azure Page Blobs with [FASTER](https://www.microsoft.com/research/project/faster/) database technology.

**Considerations:**

- ⚠️ Being deprecated—not recommended for new projects
- More complex infrastructure requirements (Event Hubs + Azure Storage)
- Consider migrating to Durable Task Scheduler for similar performance characteristics

👉 **[Netherite GitHub Repository](https://github.com/microsoft/durabletask-netherite)**

---

## Migration Between Providers

Each provider stores orchestration state differently, so migrating between providers requires:

1. **Completing or terminating** all running orchestrations
2. **Reconfiguring** the application with the new provider
3. **Restarting** orchestrations from scratch

There is no built-in state migration tool between providers.

## Need Help Deciding?

- For **enterprise support**, choose [Durable Task Scheduler](../providers/durable-task-scheduler.md)
- For **non-Azure deployments**, choose [MSSQL](https://github.com/microsoft/durabletask-mssql)
- For **lowest cost**, choose [Azure Storage](../providers/azure-storage.md)
- For **local testing**, choose [Emulator](../providers/emulator.md)

See [Support](../support.md) for more information about getting help.
