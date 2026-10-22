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

namespace DurableTask.ServiceBus.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Models;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using DurableTask.ServiceBus.Tracking;

    [TestClass]
    public class BlobStorageClientTest
    {
        private BlobStorageClient blobStorageClient;

        [TestInitialize]
        public void TestInitialize()
        {
            var r = new Random();
            string storageConnectionString = TestHelpers.GetTestSetting("StorageConnectionString");
            this.blobStorageClient = new BlobStorageClient(
                "Test00" + r.Next(0, 10000), storageConnectionString
                );
        }

        [TestCleanup]
        public async Task TestCleanup()
        {
            string storageConnectionString = TestHelpers.GetTestSetting("StorageConnectionString");
            var blobServiceClient = new BlobServiceClient(storageConnectionString);
            List<BlobContainerItem> containers = (await this.blobStorageClient.ListContainersAsync()).ToList();
            foreach (var container in containers)
            {
                var blobContainerClient = blobServiceClient.GetBlobContainerClient(container.Name);
                Assert.IsTrue(await blobContainerClient.DeleteIfExistsAsync());
            }
            containers = (await this.blobStorageClient.ListContainersAsync()).ToList();
            Assert.AreEqual(0, containers.Count);
        }

        [TestMethod]
        public async Task TestStreamBlobCreationAndDeletion()
        {
            var testContent = "test stream content";
            var key = "message-20101003|testBlobName";
            Stream stream = new MemoryStream(Encoding.UTF8.GetBytes(testContent));
            await this.blobStorageClient.UploadStreamBlobAsync(key, stream);

            var result = await this.blobStorageClient.DownloadStreamAsync(key) as MemoryStream;

            Debug.Assert(result != null);

            string resultString = Encoding.UTF8.GetString(result.ToArray());
            Assert.AreEqual(resultString, testContent);
        }

        [TestMethod]
        public async Task TestDeleteContainers()
        {
            var testContent = "test stream content";
            var key1 = "message-20150516|a";
            var key2 = "message-20150517|b";
            var key3 = "message-20150518|c";

            Stream stream = new MemoryStream(Encoding.UTF8.GetBytes(testContent));
            await this.blobStorageClient.UploadStreamBlobAsync(key1, stream);
            stream.Position = 0;
            await this.blobStorageClient.UploadStreamBlobAsync(key2, stream);
            stream.Position = 0;
            await this.blobStorageClient.UploadStreamBlobAsync(key3, stream);
            stream.Position = 0;

            var dateTime = new DateTime(2015, 05, 17);
            await this.blobStorageClient.DeleteExpiredContainersAsync(dateTime);

            List<BlobContainerItem> containers = (await this.blobStorageClient.ListContainersAsync()).ToList();
            Assert.AreEqual(2, containers.Count);
            var sortedList = new List<string> {containers[0].Name, containers[1].Name};
            sortedList.Sort();

            Assert.IsTrue(sortedList[0].EndsWith("20150517"));
            Assert.IsTrue(sortedList[1].EndsWith("20150518"));

            await this.blobStorageClient.DeleteBlobStoreContainersAsync();
            containers = (await this.blobStorageClient.ListContainersAsync()).ToList();
            Assert.AreEqual(0, containers.Count);
        }

        [TestMethod]
        public async Task TestDeleteExpiredContainersPreservesOverlappingHub()
        {
            // "{hub}-dtfx-backup" shares the container name prefix "{hub}-dtfx" with "{hub}",
            // so listing "{hub}" containers also returns the other hub's containers.
            string storageConnectionString = TestHelpers.GetTestSetting("StorageConnectionString");
            var serviceClient = new BlobServiceClient(storageConnectionString);
            string hubName = "alpha" + Guid.NewGuid().ToString("N").Substring(0, 8);
            string overlappingHubName = hubName + "-dtfx-backup";
            var hubClient = new BlobStorageClient(hubName, storageConnectionString);
            var overlappingHubClient = new BlobStorageClient(overlappingHubName, storageConnectionString);

            string hubPrefix = BlobStorageClientHelper.BuildContainerNamePrefix(hubName);
            string overlappingPrefix = BlobStorageClientHelper.BuildContainerNamePrefix(overlappingHubName);
            string hubExpired = BlobStorageClientHelper.BuildContainerName(hubPrefix, "message-20150516");
            string hubCurrent = BlobStorageClientHelper.BuildContainerName(hubPrefix, "message-20150518");
            string overlappingExpiredMessage = BlobStorageClientHelper.BuildContainerName(overlappingPrefix, "message-20150516");
            string overlappingExpiredSession = BlobStorageClientHelper.BuildContainerName(overlappingPrefix, "session-20150516");

            try
            {
                await UploadAsync(hubClient, "message-20150516|a");
                await UploadAsync(hubClient, "message-20150518|b");
                await UploadAsync(overlappingHubClient, "message-20150516|c");
                await UploadAsync(overlappingHubClient, "session-20150516|d");

                await hubClient.DeleteExpiredContainersAsync(new DateTime(2015, 05, 17));

                Assert.IsFalse(await serviceClient.GetBlobContainerClient(hubExpired).ExistsAsync(), "The hub's expired container must be deleted.");
                Assert.IsTrue(await serviceClient.GetBlobContainerClient(hubCurrent).ExistsAsync(), "The hub's current container must be kept.");
                Assert.IsTrue(await serviceClient.GetBlobContainerClient(overlappingExpiredMessage).ExistsAsync(), "The other hub's message container must be kept.");
                Assert.IsTrue(await serviceClient.GetBlobContainerClient(overlappingExpiredSession).ExistsAsync(), "The other hub's session container must be kept.");

                Stream session = await overlappingHubClient.DownloadStreamAsync("session-20150516|d");
                Assert.AreEqual("test stream content", new StreamReader(session).ReadToEnd());
            }
            finally
            {
                foreach (string name in new[] { hubExpired, hubCurrent, overlappingExpiredMessage, overlappingExpiredSession })
                {
                    await serviceClient.GetBlobContainerClient(name).DeleteIfExistsAsync();
                }
            }
        }

        [TestMethod]
        public async Task TestDeleteBlobStoreContainersPreservesOverlappingHub()
        {
            string storageConnectionString = TestHelpers.GetTestSetting("StorageConnectionString");
            var serviceClient = new BlobServiceClient(storageConnectionString);
            string hubName = "alpha" + Guid.NewGuid().ToString("N").Substring(0, 8);
            string overlappingHubName = hubName + "-dtfx-backup";
            var hubClient = new BlobStorageClient(hubName, storageConnectionString);
            var overlappingHubClient = new BlobStorageClient(overlappingHubName, storageConnectionString);

            string hubPrefix = BlobStorageClientHelper.BuildContainerNamePrefix(hubName);
            string overlappingPrefix = BlobStorageClientHelper.BuildContainerNamePrefix(overlappingHubName);
            string hubMessage = BlobStorageClientHelper.BuildContainerName(hubPrefix, "message-20150516");
            string hubSession = BlobStorageClientHelper.BuildContainerName(hubPrefix, "session-20150516");
            string overlappingMessage = BlobStorageClientHelper.BuildContainerName(overlappingPrefix, "message-20150516");
            string overlappingSession = BlobStorageClientHelper.BuildContainerName(overlappingPrefix, "session-20150516");

            try
            {
                await UploadAsync(hubClient, "message-20150516|a");
                await UploadAsync(hubClient, "session-20150516|b");
                await UploadAsync(overlappingHubClient, "message-20150516|c");
                await UploadAsync(overlappingHubClient, "session-20150516|d");

                List<string> listed = (await hubClient.ListContainersAsync()).Select(c => c.Name).OrderBy(n => n).ToList();
                CollectionAssert.AreEqual(new[] { hubMessage, hubSession }, listed, "Only the hub's own containers must be listed.");

                await hubClient.DeleteBlobStoreContainersAsync();

                Assert.IsFalse(await serviceClient.GetBlobContainerClient(hubMessage).ExistsAsync(), "The hub's message container must be deleted.");
                Assert.IsFalse(await serviceClient.GetBlobContainerClient(hubSession).ExistsAsync(), "The hub's session container must be deleted.");
                Assert.IsTrue(await serviceClient.GetBlobContainerClient(overlappingMessage).ExistsAsync(), "The other hub's message container must be kept.");
                Assert.IsTrue(await serviceClient.GetBlobContainerClient(overlappingSession).ExistsAsync(), "The other hub's session container must be kept.");
            }
            finally
            {
                foreach (string name in new[] { hubMessage, hubSession, overlappingMessage, overlappingSession })
                {
                    await serviceClient.GetBlobContainerClient(name).DeleteIfExistsAsync();
                }
            }
        }

        static async Task UploadAsync(BlobStorageClient client, string key)
        {
            using (Stream stream = new MemoryStream(Encoding.UTF8.GetBytes("test stream content")))
            {
                await client.UploadStreamBlobAsync(key, stream);
            }
        }

        [TestMethod]
        public async Task TestListContainersOnlyWithPrefix()
        {
            // Create a container that does NOT use the current client's prefix
            string storageConnectionString = TestHelpers.GetTestSetting("StorageConnectionString");
            var serviceClient = new BlobServiceClient(storageConnectionString);

            string otherHub = "otherhubprefix" + Guid.NewGuid().ToString("N").Substring(0, 8);
            string otherPrefix = BlobStorageClientHelper.BuildContainerNamePrefix(otherHub);
            string otherContainerName = BlobStorageClientHelper.BuildContainerName(otherPrefix, "message-20100101");

            var otherContainer = serviceClient.GetBlobContainerClient(otherContainerName);
            await otherContainer.CreateIfNotExistsAsync();

            try
            {
                // Ensure the externally-created container is not returned by the client's listing
                List<BlobContainerItem> containers = (await this.blobStorageClient.ListContainersAsync()).ToList();
                Assert.IsFalse(containers.Any(c => string.Equals(c.Name, otherContainerName, StringComparison.OrdinalIgnoreCase)),
                    "ListContainersAsync returned a container that does not match the client's prefix.");
            }
            finally
            {
                // Clean up the external container explicitly
                await otherContainer.DeleteIfExistsAsync();
            }
        }
    }
}
