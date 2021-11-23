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

        [DataTestMethod]
        [DataRow("https://blobs", "https://queues", "https://tables")]
        [DataRow(null, "https://queues", "https://tables")]
        [DataRow("https://blobs", null, "https://tables")]
        [DataRow("https://blobs", "https://queues", null)]
        public void ToCloudStorageAccount_Endpoints(string blob, string queue, string table)
        {
            var expected = new StorageAccountDetails
            {
                AccountName = "foo",
                StorageCredentials = new StorageCredentials(),
                BlobServiceUri = blob == null ? null : new Uri(blob, UriKind.Absolute),
                QueueServiceUri = queue == null ? null : new Uri(queue, UriKind.Absolute),
                TableServiceUri = table == null ? null : new Uri(table, UriKind.Absolute),
            };

            CloudStorageAccount actual = expected.ToCloudStorageAccount();
            Assert.AreSame(expected.StorageCredentials, actual.Credentials);
            Assert.AreEqual(expected.BlobServiceUri ?? StorageAccount.GetDefaultServiceUri("foo", StorageServiceType.Blob), actual.BlobEndpoint);
            Assert.AreEqual(expected.QueueServiceUri ?? StorageAccount.GetDefaultServiceUri("foo", StorageServiceType.Queue), actual.QueueEndpoint);
            Assert.AreEqual(expected.TableServiceUri ?? StorageAccount.GetDefaultServiceUri("foo", StorageServiceType.Table), actual.TableEndpoint);
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
            Assert.AreEqual(StorageAccount.GetDefaultServiceUri("dev", StorageServiceType.Blob), actual.BlobEndpoint);
            Assert.AreEqual(StorageAccount.GetDefaultServiceUri("dev", StorageServiceType.Queue), actual.QueueEndpoint);
            Assert.AreEqual(StorageAccount.GetDefaultServiceUri("dev", StorageServiceType.Table), actual.TableEndpoint);
        }
    }
}
