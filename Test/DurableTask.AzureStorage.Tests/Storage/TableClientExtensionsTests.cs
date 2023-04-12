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
    using Azure.Data.Tables;
    using DurableTask.AzureStorage.Storage;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class TableClientExtensionsTests
    {
        const string EmulatorAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

        [TestMethod]
        public void GetUri_ConnectionString()
        {
            TableClient client;

            client = new TableClient("UseDevelopmentStorage=true", "bar");
            Assert.AreEqual(new Uri("http://127.0.0.1:10002/devstoreaccount1/bar"), client.GetUri());

            client = new TableClient($"DefaultEndpointsProtocol=https;AccountName=foo;AccountKey={EmulatorAccountKey};TableEndpoint=https://foo.table.core.windows.net/;", "bar");
            Assert.AreEqual(new Uri("https://foo.table.core.windows.net/bar"), client.GetUri());
        }

        [TestMethod]
        public void GetUri_ServiceEndpoint()
        {
            var client = new TableClient(new Uri("https://foo.table.core.windows.net/"), "bar", new TableSharedKeyCredential("foo", EmulatorAccountKey));
            Assert.AreEqual(new Uri("https://foo.table.core.windows.net/bar"), client.GetUri());
        }

        [TestMethod]
        public void GetUri_TableEndpoint()
        {
            var client = new TableClient(new Uri("https://foo.table.core.windows.net/bar"));
            Assert.AreEqual(new Uri("https://foo.table.core.windows.net/bar"), client.GetUri());
        }
    }
}