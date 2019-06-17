# Redis Storage Provider

## Current State: *Experimental*

## Current limitations
- No extended sessions support
- No durable timers support
- No history retrieval
- No suborchestration support
- No ability to terminate orchestrations
- Only one RedisOrchestrationService instance per TaskHub supported
- No Azure Functions consumption plan scaling support

## How to install Redis locally

Redis provides an excellent [quick start guide](https://redis.io/topics/quickstart) that walks through how to install and run Redis on a Linux device. For Windows machines, it is highly recommended to install the [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/faq) and to follow the Redis quickstart guide on your Linux subsystem.

## Sample code
```csharp
var settings = new RedisOrchestrationServiceSettings
{
    RedisConnectionString = "localhost:6379",
    TaskHubName = "SampleTaskHub"
};
var redisOrchestrationService = new RedisOrchestrationService(settings);
var worker = new TaskHubWorker(redisOrchestrationService);
await worker.AddTaskOrchestration(typeof(OrchestrationFunction))
    .AddTaskActivities(typeof(ActivityTask1), typeof(TaskActivity2)
    .StartAsync();
    
var client = new TaskHubClient(reidsOrchestrationService);
OrchestrationInstance instance = await client.CreateOrchestrationInstanceAsync(typeof(OrchestrationFunction), input);
OrchestrationState result = await client.WaitForOrchestrationAsync(instance, Timespan.FromSeconds(20));
```
