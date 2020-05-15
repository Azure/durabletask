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

namespace DurableTask.SqlServer.Tests
{
    using DurableTask.SqlServer.Tracking;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System.Data.Common;
    using System.Threading.Tasks;

    public abstract class BaseTestClass
    {
        protected SqlServerInstanceStore InstanceStore { get; private set; }
        protected SqlServerInstanceStoreSettings Settings { get; private set; }

        [TestInitialize]
        public void Initialize()
        {
            Settings = new SqlServerInstanceStoreSettings
            {
                GetDatabaseConnection = () => Task.FromResult<DbConnection>(DatabaseInitialization.GetDatabaseConnection()),
                HubName = "UnitTests",
                SchemaName = "durabletask"
            };

            InstanceStore = new SqlServerInstanceStore(Settings);

            InstanceStore.InitializeStoreAsync(true).Wait();
        }

        public DbConnection GetConnection() => Settings.GetDatabaseConnection().Result;
    }
}
