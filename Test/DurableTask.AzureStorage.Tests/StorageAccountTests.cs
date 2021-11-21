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

namespace DurableTask.AzureStorage.Tests
{
    [TestClass]
    public class StorageAccountTests
    {
        [TestMethod]
        public void GetDefaultServiceUriExceptions()
        {
            Assert.ThrowsException<ArgumentException>(() => StorageAccount.GetDefaultServiceUri(null, StorageService.Blob, StorageLocation.Primary));
            Assert.ThrowsException<ArgumentException>(() => StorageAccount.GetDefaultServiceUri("", StorageService.Blob, StorageLocation.Primary));
            Assert.ThrowsException<ArgumentOutOfRangeException>(() => StorageAccount.GetDefaultServiceUri("foo", (StorageService)42, StorageLocation.Primary));
            Assert.ThrowsException<ArgumentOutOfRangeException>(() => StorageAccount.GetDefaultServiceUri("foo", StorageService.Blob, (StorageLocation)42));
        }

        [DataTestMethod]
        [DataRow("foo", StorageService.Blob, StorageLocation.Primary, "https://foo.blob.core.windows.net/")]
        [DataRow("foo", StorageService.Blob, StorageLocation.Secondary, "https://foo-secondary.blob.core.windows.net/")]
        [DataRow("bar", StorageService.Queue, StorageLocation.Primary, "https://bar.queue.core.windows.net/")]
        [DataRow("bar", StorageService.Queue, StorageLocation.Secondary, "https://bar-secondary.queue.core.windows.net/")]
        [DataRow("baz", StorageService.Table, StorageLocation.Primary, "https://baz.table.core.windows.net/")]
        [DataRow("baz", StorageService.Table, StorageLocation.Secondary, "https://baz-secondary.table.core.windows.net/")]
        public void GetDefaultServiceUri(string accountName, StorageService service, StorageLocation location, string expected)
        {
            Assert.AreEqual(new Uri(expected, UriKind.Absolute), StorageAccount.GetDefaultServiceUri(accountName, service, location));
        }
    }
}
