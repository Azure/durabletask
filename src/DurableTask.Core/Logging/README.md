# DurableTask.Core Logging

DurableTask.Core supports two forms of logging:

1. [Event Source](https://docs.microsoft.com/dotnet/api/system.diagnostics.tracing.eventsource)
1. [ILogger](https://docs.microsoft.com/aspnet/core/fundamentals/logging)

**Event Source** is the high-performance logger for .NET and is used by platforms like Azure Functions and Azure App Service to automatically collect telemetry that is available to Customer Support. **ILogger** is primarily intended to allow users to collect logs for their own use (although users can also use Event Source directly, if desired). Collecting Event Source logs is outside the scope of this document, however. Both logging mechanisms expose roughly the same telemetry. Event Source captures a few additional details, such as activity IDs for high-fedelity correlation.

## ILogger configuration

Starting in DurableTask.Core v2.4.0, `TaskHubWorker` and `TaskHubClient` have constructor overloads that accept an [ILoggerFactory](https://docs.microsoft.com/dotnet/api/microsoft.extensions.logging.iloggerfactory). The `ILoggerFactory` can be used to collect logs and have them written to the console, a file, [Azure Application Insights](https://docs.microsoft.com/azure/azure-monitor/app/ilogger), or any custom location that you choose.

```csharp
// configure logging
var loggerFactory = LoggerFactory.Create(
    builder =>
    {
        // Console logging requires Microsoft.Extensions.Logging.Console 
        builder.AddSimpleConsole(options =>
        {
            options.SingleLine = true;
            options.UseUtcTimestamp = true;
            options.TimestampFormat = "yyyy-mm-ddThh:mm:ss.ffffffZ ";
        });
        
        // File logging requires Serilog.Extensions.Logging.File
        builder.AddFile("Logs/{Date}.txt", minimumLevel: LogLevel.Trace);
        
        // App Insights logging requires Microsoft.Extensions.Logging.ApplicationInsights
        builder.AddApplicationInsights(instrumentationKey);
    });

IOrchestrationService orchestrationService = GetOrchestrationService(loggerFactory);
IOrchestrationServiceClient orchestrationServiceClient = (IOrchestrationServiceClient)orchestrationService;

var worker = new TaskHubWorker(orchestrationService, loggerFactory);
var client = new TaskHubClient(orchestrationServiceClient, loggerFactory: loggerFactory);
```

Additionally, updated storage provider implementations also accept an `ILoggerFactory`. This allows you to collect provider-specific telemetry, such as message delivery time, etc., which is often necessary to debug performance and reliability issues.

**DurableTask.AzureStorage**
```csharp
IOrchestrationService GetOrchestrationService(ILoggerFactory loggerFactory)
{
    var azureStorageSettings = new AzureStorageOrchestrationServiceSettings
    {
        TaskHubName = "MyTaskHub",
        StorageConnectionString = "UseDevelopmentStorage=true",
        LoggerFactory = loggerFactory,
    };
    return new AzureStorageOrchestrationService(azureStorageSettings);
}
```

**DurableTask.SqlServer**
```csharp
IOrchestrationService GetOrchestrationService(ILoggerFactory loggerFactory)
{
    string connectionString = "Server=localhost;Database=DurableDB;Trusted_Connection=True;";
    var mssqlSettings = new SqlOrchestrationServiceSettings(connectionString)
    {
        LoggerFactory = loggerFactory,
    };
    return new SqlOrchestrationService(mssqlSettings);
}
```

## Event Source configuration

Event Source logging is always enabled and does not have any _explicit_ configuration. However, there are currently two flavors of event source logging, _structured_ and _unstructured_. By default, Event Source logs are _unstructured_. This means that every log uses a common schema such that the important information is included in a text message.

Starting in **DurableTask.Core v2.4.0**, a more modern _structured_ Event Source logging is available. Rather than logging text messages, each log event has its own unique schema, making it easier to query for very specific information when logs are stored in places like Kusto. To enable the structured Event Source logging, a non-null `loggerFactory` must be configured. Even using [NullLoggerFactory.Instance](https://docs.microsoft.com/dotnet/api/microsoft.extensions.logging.abstractions.nullloggerfactory.instance) can be used to enabled structured event source logging.

Both the legacy and structured event source providers have a provider name of `DurableTask-Core`. However, they have different provider GUID values, as shown in the following table.

| Event Source Type | Provider Name | Provider GUID |
|-|-|-|
| Structured (recommended) | [DurableTask-Core](StructuredEventSource.cs) | 413F7E86-75A9-5E9C-35DD-51DB8427E7E7 |
| Unstructured (legacy) | [DurableTask-Core](../Tracing/DefaultEventSource.cs) | 7DA4779A-152E-44A2-A6F2-F80D991A5BEE |

Note that some transaction store providers, like **DurableTask.AzureStorage** also support Event Source logging.

| Event Source Type | Provider Name | Provider GUID |
|-|-|-|
| Structured | [DurableTask-AzureStorage](/src/DurableTask.AzureStorage/AnalyticsEventSource.cs) | 4C4AD4A2-F396-5E18-01B6-618C12A10433 |
| Structured | [DurableTask-SqlServer](https://github.com/microsoft/durabletask-mssql/blob/main/src/DurableTask.SqlServer/Logging/DefaultEventSource.cs) | 4BA38912-E64F-5FD2-170D-68AC65B1E58D |

## Structured logging

There are a large number of log events emitted by DurableTask.Core. The full list, including their event IDs, verbosity, and schemas can be found in [LogEvents.cs](LogEvents.cs).

Each log event is represented as a class that derives from [StructuredLogEvent](StructuredLogEvent.cs). Each data field of the event is declared as a property with the [StructuredLogField](StructuredLogFieldAttribute.cs) attribute. If a log event supports being written to Event Source, then it also implements the [IEventSourceEvent](IEventSourceEvent.cs) interface.

Note that these classes are public. It is recommended that all Durable Task transaction store implementations leverage these classes to implement their own logging. This will enable those providers to participate in [end-to-end tracing](#end-to-end-tracing).

## Activity tracing

DurableTask.Core and the transaction store providers mentioned previously support activity tracing when writing the Event Source. This is critically important for filtering and correlating logs that are related to each other, especially when lots of operations are executing concurrently, and especially when the logs are being generated from multiple trace sources. For example, a new `ActivityId` GUID value is created every time a dispatcher attempts to fetch a new message. This trace activity value is associated with all traces within the same logical thread of execution, even across transaction store providers.

Depending on the transaction store provider (like DurableTask.AzureStorage) related activity IDs are also logged to help correlate sent messages with received messages, allowing you to easily trace the entire lifetime of an orchestration instance.
