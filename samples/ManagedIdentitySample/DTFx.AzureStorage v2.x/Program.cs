using DurableTask.AzureStorage;
using DurableTask.Core;
using Azure.Identity;

internal class Program
{
    private static async Task Main(string[] args)
    {
        var credential = new DefaultAzureCredential();
        
        // Pass the credential created to the StorageAccountClientProvider to start an AzureStorageOrchestrationService
        var service = new AzureStorageOrchestrationService(new AzureStorageOrchestrationServiceSettings
        {
            StorageAccountClientProvider = new StorageAccountClientProvider("AccountName", credential),
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
}

public class SampleOrchestration : TaskOrchestration<string, string>
{
    public override async Task<string> RunTask(OrchestrationContext context, string input)
    {
        await context.ScheduleTask<string>(typeof(SampleActivity), input);

        return "Orchestrator Finished!";
    }
}

public class SampleActivity : TaskActivity<string, string>
{
    protected override string Execute(TaskContext context, string input)
    {
        Console.WriteLine("saying hello to " + input);
        return "Hello " + input + "!";
    }
}

