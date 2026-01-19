# Installation

This guide covers installing the Durable Task Framework (DTFx) packages for your project.

## Prerequisites

- .NET 6.0 or later (.NET 10.0 is currently recommended)
- .NET Framework 4.7.2 or later (for .NET Framework projects)

## NuGet Packages

### Core Package

All DTFx applications require the core package:

```bash
dotnet add package Microsoft.Azure.DurableTask.Core
```

### Backend Providers

Backend providers implement the storage and messaging layers for DTFx. You can choose one of several backend providers based on your needs. See [Choosing a Backend](choosing-a-backend.md) for guidance.

#### Durable Task Scheduler (Recommended)

For new projects, we recommend the fully managed [Durable Task Scheduler](../providers/durable-task-scheduler.md):

```bash
dotnet add package Microsoft.DurableTask.AzureManagedBackend
```

#### Azure Storage

For self-managed deployments using Azure Storage (queues, tables, blobs):

```bash
dotnet add package Microsoft.Azure.DurableTask.AzureStorage
```

#### Azure Service Bus

For deployments using Azure Service Bus:

```bash
dotnet add package Microsoft.Azure.DurableTask.ServiceBus
```

#### Azure Service Fabric

For Service Fabric applications:

```bash
dotnet add package Microsoft.Azure.DurableTask.AzureServiceFabric
```

#### Emulator (Local Development)

For local development and testing without external dependencies:

```bash
dotnet add package Microsoft.Azure.DurableTask.Emulator
```

### Optional Packages

#### Application Insights Integration

For Application Insights telemetry:

```bash
dotnet add package Microsoft.Azure.DurableTask.ApplicationInsights
```

## Package Versions

All DTFx packages follow semantic versioning. We recommend using the latest stable versions:

| Package | NuGet |
| ------- | ----- |
| DurableTask.Core | [![NuGet](https://img.shields.io/nuget/v/Microsoft.Azure.DurableTask.Core.svg)](https://www.nuget.org/packages/Microsoft.Azure.DurableTask.Core/) |
| DurableTask.AzureManagedBackend | [![NuGet](https://img.shields.io/nuget/v/Microsoft.DurableTask.AzureManagedBackend.svg)](https://www.nuget.org/packages/Microsoft.DurableTask.AzureManagedBackend/) |
| DurableTask.AzureStorage | [![NuGet](https://img.shields.io/nuget/v/Microsoft.Azure.DurableTask.AzureStorage.svg)](https://www.nuget.org/packages/Microsoft.Azure.DurableTask.AzureStorage/) |
| DurableTask.ServiceBus | [![NuGet](https://img.shields.io/nuget/v/Microsoft.Azure.DurableTask.ServiceBus.svg)](https://www.nuget.org/packages/Microsoft.Azure.DurableTask.ServiceBus/) |
| DurableTask.AzureServiceFabric | [![NuGet](https://img.shields.io/nuget/v/Microsoft.Azure.DurableTask.AzureServiceFabric.svg)](https://www.nuget.org/packages/Microsoft.Azure.DurableTask.AzureServiceFabric/) |
| DurableTask.Emulator | [![NuGet](https://img.shields.io/nuget/v/Microsoft.Azure.DurableTask.Emulator.svg)](https://www.nuget.org/packages/Microsoft.Azure.DurableTask.Emulator/) |

## Next Steps

- [Quickstart](quickstart.md) — Create your first orchestration
- [Choosing a Backend](choosing-a-backend.md) — Compare backend providers
- [Core Concepts](../concepts/core-concepts.md) — Understand the architecture
