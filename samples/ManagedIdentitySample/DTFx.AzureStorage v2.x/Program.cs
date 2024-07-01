//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Azure.Identity;
using DurableTask.AzureStorage;
using DurableTask.Core;
using Microsoft.Extensions.Azure;
using Microsoft.Extensions.Logging;

// Create a DefaultAzureCredential used to access the Azure Storage Account.
// The identity will require the following roles on the resource:
// - Azure Blob Data Contributor
// - Azure Queue Data Contributor
// - Azure Table Data Contributor
DefaultAzureCredential credential = new();

// Create a diagnostic logger factory for reading telemetry
ILoggerFactory loggerFactory = LoggerFactory.Create(b => b
    .AddConsole()
    .AddFilter("Azure.Core", LogLevel.Warning)
    .AddFilter("Azure.Identity", LogLevel.Warning));

// The Azure SDKs used by the Azure.Identity and Azure Storage client libraries write their telemetry via Event Sources
using AzureEventSourceLogForwarder logForwarder = new(loggerFactory);
logForwarder.Start();

AzureStorageOrchestrationService service = new(new AzureStorageOrchestrationServiceSettings
{
    LoggerFactory = loggerFactory,
    StorageAccountClientProvider = new StorageAccountClientProvider("YourStorageAccount", credential),
});

TaskHubClient client = new(service, loggerFactory: loggerFactory);
TaskHubWorker worker = new(service, loggerFactory);

worker.AddTaskOrchestrations(typeof(SampleOrchestration));
worker.AddTaskActivities(typeof(SampleActivity));

await worker.StartAsync();

OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(typeof(SampleOrchestration), "World");
OrchestrationState state = await client.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(1));

ILogger logger = loggerFactory.CreateLogger(nameof(Program));
logger.LogInformation("Orchestration output: {Output}", state.Output);

await worker.StopAsync();

internal sealed class SampleOrchestration : TaskOrchestration<string, string>
{
    public override Task<string> RunTask(OrchestrationContext context, string input) =>
        context.ScheduleTask<string>(typeof(SampleActivity), input);
}

internal sealed class SampleActivity : TaskActivity<string, string>
{
    protected override string Execute(TaskContext context, string input) =>
        "Hello, " + input + "!";
}
