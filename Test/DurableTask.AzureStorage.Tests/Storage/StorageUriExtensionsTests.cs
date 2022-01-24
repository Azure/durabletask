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

namespace DurableTask.AzureStorage.Tests.Storage
{
    using System;
    using DurableTask.AzureStorage.Storage;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.WindowsAzure.Storage;

    [TestClass]
    public class StorageUriExtensionsTests
    {
        [TestMethod]
        [DataRow("https://foo.blob.core.windows.net", "blob", "foo")]
        [DataRow("https://bar.queue.unit.test", "queue", "bar")]
        [DataRow("https://baz.table", "table", "baz")]
        [DataRow("https://dev.account.file.core.windows.net", "file", "dev")]
        [DataRow("https://dev.unknown.core.windows.net", "blob", null)]
        [DataRow("https://host/path", "blob", null)]
        [DataRow("http://127.0.0.1:10000/devstoreaccount1", "file", "devstoreaccount1")]
        [DataRow("http://host:10102/devstoreaccount2/more", "blob", "devstoreaccount2")]
        [DataRow("http://unit.test/custom/uri", "queue", null)]
        public void GetAccountName(string uri, string service, string accountName) =>
            Assert.AreEqual(accountName, new StorageUri(new Uri(uri, UriKind.Absolute)).GetAccountName(service));
    }
}
