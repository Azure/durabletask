using DurableTask.SqlServer.Tracking;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Data.Common;
using System.Threading.Tasks;

namespace DurableTask.SqlServer.Tests
{
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
