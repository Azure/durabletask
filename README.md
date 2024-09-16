# Durable Task Framework

The Durable Task Framework (DTFx) is a library that allows users to write long running persistent workflows (referred to as _orchestrations_) in C# using simple async/await coding constructs. It is used heavily within various teams at Microsoft to reliably orchestrate long running provisioning, monitoring, and management operations. The orchestrations scale out linearly by simply adding more worker machines. This framework is also used to power the serverless [Durable Functions](https://docs.microsoft.com/azure/azure-functions/durable/durable-functions-overview) extension of [Azure Functions](https://azure.microsoft.com/services/functions/).

By open sourcing this project we hope to give the community a very cost-effective alternative to heavy duty workflow systems. We also hope to build an ecosystem of providers and activities around this simple yet incredibly powerful framework.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).

For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Supported persistance stores

Starting in v2.x, the Durable Task Framework supports an extensible set of backend persistence stores. Each store can be enabled using a different NuGet package. The latest version of all packages are signed and available for download at nuget.org.

| Package | Latest Version | Details | Development Status |
| ------- | -------------- | ------- | ------------------ |
| DurableTask.ServiceBus | [![NuGet](https://img.shields.io/nuget/v/Microsoft.Azure.DurableTask.ServiceBus.svg?style=flat)](https://www.nuget.org/packages/Microsoft.Azure.DurableTask.ServiceBus/) | Orchestration message and runtime state is stored in Service Bus queues while tracking state is stored in Azure Storage. The strength of this provider is its maturity and transactional consistency. However, it is no longer in active development at Microsoft. | Production ready but not actively maintained |
| DurableTask.AzureStorage | [![NuGet](https://img.shields.io/nuget/v/Microsoft.Azure.DurableTask.AzureStorage.svg?style=flat)](https://www.nuget.org/packages/Microsoft.Azure.DurableTask.AzureStorage/) | All orchestration state is stored in Azure Storage queues, tables, and blobs. The strength of this provider is the minimal service dependencies, high efficiency, and rich feature-set. This backend is available for [Durable Functions](https://docs.microsoft.com/azure/azure-functions/durable/). | Production ready and actively maintained |
| DurableTask.AzureServiceFabric | [![NuGet](https://img.shields.io/nuget/v/Microsoft.Azure.DurableTask.AzureServiceFabric.svg?style=flat)](https://www.nuget.org/packages/Microsoft.Azure.DurableTask.AzureServiceFabric/) | All orchestration state is stored in [Azure Service Fabric Reliable Collections](https://docs.microsoft.com/azure/service-fabric/service-fabric-reliable-services-reliable-collections). This is an ideal choice if you are hosting your application in [Azure Service Fabric](https://azure.microsoft.com/services/service-fabric/) and don't want to take on external dependencies for storing state. | Production ready and actively maintained |
| DurableTask.Netherite | [![NuGet](https://img.shields.io/nuget/v/Microsoft.Azure.DurableTask.Netherite.svg?style=flat)](https://www.nuget.org/packages/Microsoft.Azure.DurableTask.Netherite/) | An ultra-high performance backend developed by Microsoft Research where state is stored in [Azure Event Hubs](https://azure.microsoft.com/en-us/services/event-hubs/) and Azure Page Blobs using [FASTER](https://www.microsoft.com/research/project/faster/) database technology from Microsoft Research. This backend is available for [Durable Functions](https://docs.microsoft.com/azure/azure-functions/durable/). [👉 GitHub Repo](https://github.com/microsoft/durabletask-netherite) | Production ready and actively maintained |
| DurableTask.SqlServer | [![NuGet](https://img.shields.io/nuget/v/Microsoft.DurableTask.SqlServer.svg?style=flat)](https://www.nuget.org/packages/Microsoft.DurableTask.SqlServer/) | All orchestration state is stored in a [Microsoft SQL Server](https://www.microsoft.com/sql-server/sql-server-2019) or [Azure SQL](https://azure.microsoft.com/products/azure-sql/database/) database with indexed tables and stored procedures for direct interaction. This backend is available for [Durable Functions](https://docs.microsoft.com/azure/azure-functions/durable/). [👉 GitHub Repo](https://github.com/microsoft/durabletask-mssql) | Production ready and actively maintained |
| DurableTask.Emulator | [![NuGet](https://img.shields.io/nuget/v/Microsoft.Azure.DurableTask.Emulator.svg?style=flat)](https://www.nuget.org/packages/Microsoft.Azure.DurableTask.Emulator/) | This is an in-memory store intended for testing purposes only. It is not designed or recommended for any production workloads. | Not actively maintained |

The core programming model for the Durable Task Framework is contained in the [DurableTask.Core](https://www.nuget.org/packages/Microsoft.Azure.DurableTask.Core/) package, which is also under active development.

## Learning more

There are several places where you can learn more about this framework. Note that some are external and not owned by Microsoft:

- [This repo's wiki](https://github.com/Azure/durabletask/wiki), which contains more details about the framework and how it can be used.
- The following blog series contains useful information: https://abhikmitra.github.io/blog/durable-task/
- Several useful samples are available here: https://github.com/kaushiksk/durabletask-samples
- You can watch a video with some of the original maintainers in [Building Workflows with the Durable Task Framework](https://learn.microsoft.com/shows/on-net/building-workflows-with-the-durable-task-framework).
- In some cases, the [Azure Durable Functions documentation](https://learn.microsoft.com/azure/azure-functions/durable/) can actually be useful in learning things about the underlying framework, although not everything will apply.

## Development Notes

To run unit tests, you must specify your Service Bus connection string for the tests to use. You can do this via the **ServiceBusConnectionString** app.config value in the test project, or by defining a **DurableTaskTestServiceBusConnectionString** environment variable. The benefit of the environment variable is that no temporary source changes are required.

Unit tests also require [Azure Storage Emulator](https://docs.microsoft.com/azure/storage/common/storage-use-emulator), so make sure it's installed and running.

> Note: While it's possible to use in tests a real Azure Storage account it is not recommended to do so because many tests will fail with a 409 Conflict error. This is because tests delete and quickly recreate the same storage tables, and Azure Storage doesn't do well in these conditions. If you really want to change Azure Storage connection string you can do so via the **StorageConnectionString** app.config value in the test project, or by defining a **DurableTaskTestStorageConnectionString** environment variable. 

<!--
There is a gitter for this repo, but it's not currently being monitored. We're leaving the link for it up for now and will update this message if anything changes.

[![Join the chat at https://gitter.im/azure/durabletask](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/azure/durabletask?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) 
-->
