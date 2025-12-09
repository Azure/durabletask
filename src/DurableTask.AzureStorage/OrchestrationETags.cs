using Azure;

#nullable enable
namespace DurableTask.AzureStorage
{
    class OrchestrationETags
    {
        internal ETag? InstanceETag { get; set; }

        internal ETag? HistoryETag { get; set; }
    }
}
