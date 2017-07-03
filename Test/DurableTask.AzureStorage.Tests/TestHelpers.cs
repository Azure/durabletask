using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.AzureStorage.Tests
{
    static class TestHelpers
    {
        public static TestOrchestrationHost GetTestOrchestrationHost()
        {
            string storageConnectionString = GetTestStorageAccountConnectionString();

            var service = new AzureStorageOrchestrationService(
                new AzureStorageOrchestrationServiceSettings
                {
                    StorageConnectionString = storageConnectionString,
                    TaskHubName = ConfigurationManager.AppSettings.Get("TaskHubName"),
                });

            service.CreateAsync().GetAwaiter().GetResult();

            return new TestOrchestrationHost(service);
        }

        public static string GetTestStorageAccountConnectionString()
        {
            string storageConnectionString = GetTestSetting("StorageConnectionString");
            if (string.IsNullOrEmpty(storageConnectionString))
            {
                throw new ArgumentNullException("A Storage connection string must be defined in either an environment variable or in configuration.");
            }

            return storageConnectionString;
        }

        static string GetTestSetting(string name)
        {
            string value = Environment.GetEnvironmentVariable("DurableTaskTest" + name);
            if (string.IsNullOrEmpty(value))
            {
                value = ConfigurationManager.AppSettings.Get(name);
            }

            return value;
        }
    }
}
