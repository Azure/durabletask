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
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class AzureStorageOrchestrationServiceSettingsTest
    {
        [TestMethod]
        public void TrackingStoreStorageAccountDetailsNotSet()
        {
            var settings = new AzureStorageOrchestrationServiceSettings()
            {
                TaskHubName = "foo",
            };
            Assert.AreEqual("fooHistory", settings.HistoryTableName);
            Assert.AreEqual("fooInstances", settings.InstanceTableName);
        }

        [TestMethod]
        public void TrackingStoreStorageAccountDetailsHasSet()
        {
            var settings = new AzureStorageOrchestrationServiceSettings()
            {
                TaskHubName = "foo",
                TrackingStoreNamePrefix = "bar",
                TrackingStoreStorageAccountDetails = new StorageAccountDetails()
            };
            Assert.AreEqual("barHistory", settings.HistoryTableName);
            Assert.AreEqual("barInstances", settings.InstanceTableName);
        }
    }
}
