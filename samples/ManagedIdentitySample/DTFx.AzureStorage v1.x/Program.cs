using Azure.Core;
using Azure.Identity;
using DurableTask.AzureStorage;
using DurableTask.Core;
using Microsoft.WindowsAzure.Storage.Auth;

internal class Program
{
    private static async Task Main(string[] args)
    {

        // Create credential based on the configuration
        var credential = new DefaultAzureCredential();
        string[] scopes = new string[] { "https://storage.azure.com/.default" };  // Scope for Azure Storage

        static Task<NewTokenAndFrequency> RenewTokenFuncAsync(object state, CancellationToken cancellationToken)
        {
            var credential = new DefaultAzureCredential();
            var initialToken = credential.GetToken(new TokenRequestContext(new[] { "https://storage.azure.com/.default" }));
            var expiresAfter = initialToken.ExpiresOn - DateTimeOffset.UtcNow - TimeSpan.FromMinutes(10);
            return Task.FromResult(new NewTokenAndFrequency(initialToken.Token, expiresAfter));
        }


        // Get the token
        var accessToken = await credential.GetTokenAsync(new Azure.Core.TokenRequestContext(scopes));
        
        var acredential = new Microsoft.WindowsAzure.Storage.Auth.TokenCredential(
              accessToken.Token,
              RenewTokenFuncAsync,
              null,
              TimeSpan.FromMinutes(5));
        var service = new AzureStorageOrchestrationService(new AzureStorageOrchestrationServiceSettings
        {
            StorageAccountDetails = new StorageAccountDetails
            {
                AccountName = "YourStorageAccount",
                EndpointSuffix = "core.windows.net",
                StorageCredentials = new StorageCredentials(new Microsoft.WindowsAzure.Storage.Auth.TokenCredential(
                    accessToken.Token,
                    RenewTokenFuncAsync,
                    null,
                    TimeSpan.FromMinutes(5)))
            }
        });
        var client = new TaskHubClient(service);
        var worker = new TaskHubWorker(service);
        worker.AddTaskOrchestrations(typeof(SampleOrchestration));
        worker.AddTaskActivities(typeof(SampleActivity));

        await worker.StartAsync();
        var instance = await client.CreateOrchestrationInstanceAsync(typeof(SampleOrchestration), "World");

        var result = await client.WaitForOrchestrationAsync(instance, TimeSpan.FromMinutes(1));

        Console.WriteLine($"Orchestration result : {result.Output}");

        await worker.StopAsync();
    }

    public static Task<NewTokenAndFrequency> RenewTokenFuncAsync(object state, CancellationToken cancellationToken)
    {
        var credential = new DefaultAzureCredential();
        var initialToken = credential.GetToken(new TokenRequestContext(new[] { "https://storage.azure.com/.default" }));
        var expiresAfter = initialToken.ExpiresOn - DateTimeOffset.UtcNow - TimeSpan.FromMinutes(10);
        return Task.FromResult(new NewTokenAndFrequency(initialToken.Token, expiresAfter));
    }
}

public class SampleOrchestration : TaskOrchestration<string, string>
{
    public override async Task<string> RunTask(OrchestrationContext context, string input)
    {
        return await context.ScheduleTask<string>(typeof(SampleActivity), input);
    }
}

public class SampleActivity : TaskActivity<string, string>
{
    protected override string Execute(TaskContext context, string input)
    {
        return "Hello, " + input + "!";
    }
}
