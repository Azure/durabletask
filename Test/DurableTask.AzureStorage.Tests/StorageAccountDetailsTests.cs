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

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;

namespace DurableTask.AzureStorage.Tests
{
    [TestClass]
    public class StorageAccountDetailsTests
    {
        [TestMethod]
        public void ToCloudStorageAccount_ConnectionString()
        {
            var details = new StorageAccountDetails { ConnectionString = "UseDevelopmentStorage=true" };

            CloudStorageAccount actual = details.ToCloudStorageAccount();
            Assert.AreEqual(new Uri("http://127.0.0.1:10000/devstoreaccount1", UriKind.Absolute), actual.BlobEndpoint);
            Assert.AreEqual(new Uri("http://127.0.0.1:10001/devstoreaccount1", UriKind.Absolute), actual.QueueEndpoint);
            Assert.AreEqual(new Uri("http://127.0.0.1:10002/devstoreaccount1", UriKind.Absolute), actual.TableEndpoint);
        }

        [TestMethod]
        public void ToCloudStorageAccount_Endpoints()
        {
            var expected = new StorageAccountDetails
            {
                StorageCredentials = new StorageCredentials(),
                BlobServiceUri = new Uri("https://blobs", UriKind.Absolute),
                QueueServiceUri = new Uri("https://queues", UriKind.Absolute),
                TableServiceUri = new Uri("https://tables", UriKind.Absolute),
            };

            CloudStorageAccount actual = expected.ToCloudStorageAccount();
            Assert.AreSame(expected.StorageCredentials, actual.Credentials);
            Assert.AreEqual(expected.BlobServiceUri, actual.BlobEndpoint);
            Assert.AreEqual(expected.QueueServiceUri, actual.QueueEndpoint);
            Assert.AreEqual(expected.TableServiceUri, actual.TableEndpoint);
        }

        [DataTestMethod]
        [DataRow(true, false, false)]
        [DataRow(false, true, false)]
        [DataRow(false, false, true)]
        [DataRow(true, true, false)]
        [DataRow(false, true, true)]
        [DataRow(true, false, true)]
        public void ToCloudStorageAccount_EndpointSubset(bool hasBlob, bool hasQueue, bool hasTable)
        {
            var expected = new StorageAccountDetails
            {
                StorageCredentials = new StorageCredentials(),
                BlobServiceUri = hasBlob ? new Uri("https://blobs", UriKind.Absolute) : null,
                QueueServiceUri = hasQueue ? new Uri("https://queues", UriKind.Absolute) : null,
                TableServiceUri = hasTable ? new Uri("https://tables", UriKind.Absolute) : null,
            };

            Assert.ThrowsException<InvalidOperationException>(() => expected.ToCloudStorageAccount());
        }

        [TestMethod]
        public void ToCloudStorageAccount_AccountName()
        {
            var expected = new StorageAccountDetails
            {
                AccountName = "dev",
                StorageCredentials = new StorageCredentials(),
            };

            CloudStorageAccount actual = expected.ToCloudStorageAccount();
            Assert.AreSame(expected.StorageCredentials, actual.Credentials);
            Assert.AreEqual(new Uri("https://dev.blob.core.windows.net", UriKind.Absolute), actual.BlobEndpoint);
            Assert.AreEqual(new Uri("https://dev.queue.core.windows.net", UriKind.Absolute), actual.QueueEndpoint);
            Assert.AreEqual(new Uri("https://dev.table.core.windows.net", UriKind.Absolute), actual.TableEndpoint);
        }
    }
}
