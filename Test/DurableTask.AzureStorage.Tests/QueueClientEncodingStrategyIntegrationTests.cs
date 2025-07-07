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

namespace DurableTask.AzureStorage.Tests
{
    using DurableTask.AzureStorage;
    using DurableTask.Core;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;

    [TestClass]
    public class QueueClientMessageEncodingIntegrationTests
    {
        private const string TestConnectionString = "UseDevelopmentStorage=true";

        [TestMethod]
        // Verifies that messages sent with Base64 encoding can be processed by a worker with UTF8 encoding
        public async Task CrossEncodingCompatibility_Base64ToUtf8()
        {
            string testName = "Base64ToUtf8";
            string input = "世界! 🌍 Test with émojis and spéciål chàracters: ñáéíóúü";

            // Create service with Base64 encoding to send messages
            var base64Settings = new AzureStorageOrchestrationServiceSettings
            {
                TaskHubName = testName,
                StorageAccountClientProvider = new StorageAccountClientProvider(TestConnectionString),
                QueueClientMessageEncoding = QueueClientMessageEncoding.Base64,
            };

            var base64Service = new AzureStorageOrchestrationService(base64Settings);
            await base64Service.CreateIfNotExistsAsync();
            // DON'T start the service - this prevents the dequeue loop from running

            try
            {
                // Create orchestration instance with Base64 encoding
                var base64Client = new TaskHubClient(base64Service);
                var instance = await base64Client.CreateOrchestrationInstanceAsync(typeof(HelloOrchestrator), input);

                // Create worker with UTF8 encoding to process the message
                var utf8Settings = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageAccountClientProvider = new StorageAccountClientProvider(TestConnectionString),
                    QueueClientMessageEncoding = QueueClientMessageEncoding.UTF8,
                };

                var utf8Service = new AzureStorageOrchestrationService(utf8Settings);
                var utf8Client = new TaskHubClient(utf8Service);
                var worker = new TaskHubWorker(utf8Service);
                worker.AddTaskOrchestrations(typeof(HelloOrchestrator));
                worker.AddTaskActivities(typeof(Hello));

                await worker.StartAsync();

                try
                {
                    // Wait for the orchestration to complete using UTF8 worker
                    var state = await utf8Client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(60));
                    
                    // Verify UTF8 worker successfully processed the Base64 encoded message
                    Assert.IsNotNull(state);
                    Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
                    Assert.AreEqual($"\"Hello, {input}!\"", state.Output);
                }
                finally
                {
                    await worker.StopAsync();
                }
            }
            finally
            {
                await base64Service.DeleteAsync();
            }
        }

        [TestMethod]
        // Verifies that messages sent with UTF8 encoding can be processed by a worker with Base64 encoding
        public async Task CrossEncodingCompatibility_Utf8ToBase64()
        {
            string testName = "Utf8ToBase64test0";
            string input = "世界! 🌍 Test with émojis and spéciål chàracters: ñáéíóúü";

            // Create service with UTF8 encoding to send messages
            var utf8Settings = new AzureStorageOrchestrationServiceSettings
            {
                TaskHubName = testName,
                StorageAccountClientProvider = new StorageAccountClientProvider(TestConnectionString),
                QueueClientMessageEncoding = QueueClientMessageEncoding.UTF8,
            };

            var utf8Service = new AzureStorageOrchestrationService(utf8Settings);
            await utf8Service.CreateIfNotExistsAsync();
            // DON'T start the service - this prevents the dequeue loop from running

            try
            {
                // Create orchestration instance with UTF8 encoding
                var utf8Client = new TaskHubClient(utf8Service);
                var instance = await utf8Client.CreateOrchestrationInstanceAsync(typeof(HelloOrchestrator), input);

                // Create worker with Base64 encoding to process the message
                var base64Settings = new AzureStorageOrchestrationServiceSettings
                {
                    TaskHubName = testName,
                    StorageAccountClientProvider = new StorageAccountClientProvider(TestConnectionString),
                    QueueClientMessageEncoding = QueueClientMessageEncoding.Base64,
                };

                var base64Service = new AzureStorageOrchestrationService(base64Settings);
                var base64Client = new TaskHubClient(base64Service);
                var worker = new TaskHubWorker(base64Service);
                worker.AddTaskOrchestrations(typeof(HelloOrchestrator));
                worker.AddTaskActivities(typeof(Hello));

                await worker.StartAsync();

                try
                {
                    // Wait for the orchestration to complete using Base64 worker
                    var state = await base64Client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(60));
                    
                    // Verify Base64 worker successfully processed the UTF8 encoded message
                    Assert.IsNotNull(state);
                    Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
                    Assert.AreEqual($"\"Hello, {input}!\"", state.Output);
                }
                finally
                {
                    await worker.StopAsync();
                }
            }
            finally
            {
                await utf8Service.DeleteAsync();
            }
        }

        [TestMethod]
        // Verifies that messages sent with Base64 encoding can be processed by a worker with Base64 encoding
        public async Task SameEncodingStrategy_Base64ToBase64()
        {
            string testName = "Base64ToBase64";
            string input = "世界! 🌍 Test with émojis and spéciål chàracters: ñáéíóúü";

            var base64Settings = new AzureStorageOrchestrationServiceSettings
            {
                TaskHubName = testName,
                StorageAccountClientProvider = new StorageAccountClientProvider(TestConnectionString),
                QueueClientMessageEncoding = QueueClientMessageEncoding.Base64,
            };

            var base64Service = new AzureStorageOrchestrationService(base64Settings);

            try
            {
                var base64Client = new TaskHubClient(base64Service);
                var worker = new TaskHubWorker(base64Service);
                worker.AddTaskOrchestrations(typeof(HelloOrchestrator));
                worker.AddTaskActivities(typeof(Hello));

                await worker.StartAsync();

                try
                {
                    var instance = await base64Client.CreateOrchestrationInstanceAsync(typeof(HelloOrchestrator), input);
                    var state = await base64Client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(60));

                    // Verify Base64 worker successfully processed Base64 encoded message
                    Assert.IsNotNull(state);
                    Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
                    Assert.AreEqual($"\"Hello, {input}!\"", state.Output);
                }
                finally
                {
                    await worker.StopAsync();
                }
            }
            finally
            {
                await base64Service.DeleteAsync();
            }
        }

        [TestMethod]
        // Verifies that messages sent with UTF8 encoding can be processed by a worker with UTF8 encoding
        public async Task SameEncodingStrategy_Utf8ToUtf8()
        {
            string testName = "Utf8ToUtf8";
            string input = "世界! 🌍 Test with émojis and spéciål chàracters: ñáéíóúü";

            var utf8Settings = new AzureStorageOrchestrationServiceSettings
            {
                TaskHubName = testName,
                StorageAccountClientProvider = new StorageAccountClientProvider(TestConnectionString),
                QueueClientMessageEncoding = QueueClientMessageEncoding.UTF8,
            };

            var utf8Service = new AzureStorageOrchestrationService(utf8Settings);

            try
            {
                var utf8Client = new TaskHubClient(utf8Service);
                var worker = new TaskHubWorker(utf8Service);
                worker.AddTaskOrchestrations(typeof(HelloOrchestrator));
                worker.AddTaskActivities(typeof(Hello));

                await worker.StartAsync();

                try
                {
                    var instance = await utf8Client.CreateOrchestrationInstanceAsync(typeof(HelloOrchestrator), input);
                    var state = await utf8Client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(60));

                    // Verify UTF8 worker successfully processed UTF8 encoded message
                    Assert.IsNotNull(state);
                    Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
                    Assert.AreEqual($"\"Hello, {input}!\"", state.Output);
                }
                finally
                {
                    await worker.StopAsync();
                }
            }
            finally
            {
                await utf8Service.DeleteAsync();
            }
        }

        [TestMethod]
        // Verifies that Base64 encoding can handle non-UTF8 characters like 0xFFFE (Byte Order Mark)
        public async Task Base64Encodig_HandlesNonUtf8Characters()
        {
            string testName = "Base64WithUtf8Chars";
            // Create a string with non-UTF8 characters including 0xFFFE (Byte Order Mark)
            string input = "Normal text " + (char)0xFFFE + " with BOM and " + (char)0xFFFF + " other invalid chars";

            var base64Settings = new AzureStorageOrchestrationServiceSettings
            {
                TaskHubName = testName,
                StorageAccountClientProvider = new StorageAccountClientProvider(TestConnectionString),
                QueueClientMessageEncoding = QueueClientMessageEncoding.Base64,
            };

            var base64Service = new AzureStorageOrchestrationService(base64Settings);

            try
            {
                var base64Client = new TaskHubClient(base64Service);
                var worker = new TaskHubWorker(base64Service);
                worker.AddTaskOrchestrations(typeof(HelloOrchestrator));
                worker.AddTaskActivities(typeof(Hello));

                await worker.StartAsync();

                try
                {
                    var instance = await base64Client.CreateOrchestrationInstanceAsync(typeof(HelloOrchestrator), input);
                    var state = await base64Client.WaitForOrchestrationAsync(instance, TimeSpan.FromSeconds(60));

                    // Verify Base64 encoding successfully handled non-UTF8 characters
                    Assert.IsNotNull(state);
                    Assert.AreEqual(OrchestrationStatus.Completed, state.OrchestrationStatus);
                    Assert.AreEqual($"\"Hello, {input}!\"", state.Output);
                }
                finally
                {
                    await worker.StopAsync();
                }
            }
            finally
            {
                await base64Service.DeleteAsync();
            }
        }

        // Test orchestrator and activity
        [KnownType(typeof(Hello))]
        internal class HelloOrchestrator : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                // Wait for 5 seconds before executing the activity (shorter for faster tests)
                // await context.CreateTimer<object>(context.CurrentUtcDateTime.AddSeconds(5), null);
                return await context.ScheduleTask<string>(typeof(Hello), input);
            }
        }

        internal class Hello : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                if (string.IsNullOrEmpty(input))
                {
                    throw new ArgumentNullException(nameof(input));
                }

                return $"Hello, {input}!";
            }
        }
    }
} 