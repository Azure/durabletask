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
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage.Blob;
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
            List<CloudBlobContainer> containers = (await this.blobStorageClient.ListContainers()).ToList();
            foreach (var container in containers)
            {
                Assert.IsTrue(await container.DeleteIfExistsAsync());
            }
            containers = (await this.blobStorageClient.ListContainers()).ToList();
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
            await this.blobStorageClient.UploadStreamBlobAsync(key2, stream);
            await this.blobStorageClient.UploadStreamBlobAsync(key3, stream);

            var dateTime = new DateTime(2015, 05, 17);
            await this.blobStorageClient.DeleteExpiredContainersAsync(dateTime);

            List<CloudBlobContainer> containers = (await this.blobStorageClient.ListContainers()).ToList();
            Assert.AreEqual(2, containers.Count);
            var sortedList = new List<string> {containers[0].Name, containers[1].Name};
            sortedList.Sort();

            Assert.IsTrue(sortedList[0].EndsWith("20150517"));
            Assert.IsTrue(sortedList[1].EndsWith("20150518"));

            await this.blobStorageClient.DeleteBlobStoreContainersAsync();
            containers = (await this.blobStorageClient.ListContainers()).ToList();
            Assert.AreEqual(0, containers.Count);
        }
    }
}
