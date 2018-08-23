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
            blobStorageClient = new BlobStorageClient(
                "Test00" + r.Next(0, 10000), storageConnectionString
                );
        }

        [TestCleanup]
        public void TestCleanup()
        {
            List<CloudBlobContainer> containers = blobStorageClient.ListContainers().ToList();
            containers.ForEach(container => container.DeleteIfExists());
            containers = blobStorageClient.ListContainers().ToList();
            Assert.AreEqual(0, containers.Count);
        }

        [TestMethod]
        public async Task TestStreamBlobCreationAndDeletion()
        {
            string testContent = "test stream content";
            string key = "message-20101003|testBlobName";
            Stream stream = new MemoryStream(Encoding.UTF8.GetBytes(testContent));
            await blobStorageClient.UploadStreamBlobAsync(key, stream);

            MemoryStream result = await blobStorageClient.DownloadStreamAsync(key) as MemoryStream;
            string resultString = Encoding.UTF8.GetString(result.ToArray());
            Assert.AreEqual(resultString, testContent);
        }

        [TestMethod]
        public async Task TestDeleteContainers()
        {
            string testContent = "test stream content";
            string key1 = "message-20150516|a";
            string key2 = "message-20150517|b";
            string key3 = "message-20150518|c";

            Stream stream = new MemoryStream(Encoding.UTF8.GetBytes(testContent));
            await blobStorageClient.UploadStreamBlobAsync(key1, stream);
            await blobStorageClient.UploadStreamBlobAsync(key2, stream);
            await blobStorageClient.UploadStreamBlobAsync(key3, stream);

            DateTime dateTime = new DateTime(2015, 05, 17);
            await blobStorageClient.DeleteExpiredContainersAsync(dateTime);

            List<CloudBlobContainer> containers = blobStorageClient.ListContainers().ToList();
            Assert.AreEqual(2, containers.Count);
            List<string> sortedList = new List<string> {containers[0].Name, containers[1].Name};
            sortedList.Sort();

            Assert.IsTrue(sortedList[0].EndsWith("20150517"));
            Assert.IsTrue(sortedList[1].EndsWith("20150518"));

            await blobStorageClient.DeleteBlobStoreContainersAsync();
            containers = blobStorageClient.ListContainers().ToList();
            Assert.AreEqual(0, containers.Count);
        }
    }
}
