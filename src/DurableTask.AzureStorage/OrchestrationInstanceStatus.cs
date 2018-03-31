using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.AzureStorage
{
    class OrchestrationInstanceStatus : TableEntity
    {
        public string ExecutionId { get; set; }
        public string Name { get; set; }
        public string Version { get; set; }
        public string Input { get; set; }
        public string InputBlobName { get; set; }
        public string Output { get; set; }
        public string OutputBlobName { get; set; }
        public string CustomStatus { get; set; }
        public DateTime CreatedTime { get; set; }
        public DateTime LastUpdatedTime { get; set; }
        public string RuntimeStatus { get; set; }
    }
}
