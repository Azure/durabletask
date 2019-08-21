using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Threading.Tasks;

namespace DurableTask.SqlServer.Tests
{
    [TestClass]
    public class InstanceStoreSetupTeardownTests : BaseTestClass
    {
        [TestMethod]
        public async Task InitializeStoreIdempotenceTest()
        {
            await InstanceStore.InitializeStoreAsync(true);
            await InstanceStore.InitializeStoreAsync(false);
        }

        [TestMethod]
        public async Task DeleteStoreIdempotenceTest()
        {
            await InstanceStore.DeleteStoreAsync();
            await InstanceStore.DeleteStoreAsync();
        }

        [TestMethod]
        public async Task InitializeStoreVerifyTablesExistTest()
        {
            await InstanceStore.InitializeStoreAsync(false);

            using (var connection = GetConnection())
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $"SELECT OBJECT_ID('{Settings.OrchestrationStateTableName}'), OBJECT_ID('{Settings.WorkItemTableName}')";

                await connection.OpenAsync();
                var reader = await command.ExecuteReaderAsync();

                if (await reader.ReadAsync())
                {
                    Assert.AreNotEqual(DBNull.Value, reader.GetValue(0), "No Orchestration table found.");
                    Assert.AreNotEqual(DBNull.Value, reader.GetValue(1), "No Work Item table found.");
                    return;
                }
            }

            Assert.Fail("No rows returned by query.");
        }

        [TestMethod]
        public async Task DeleteStoreVerifyTablesDoNotExistTest()
        {
            await InstanceStore.DeleteStoreAsync();

            using (var connection = GetConnection())
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $"SELECT OBJECT_ID('{Settings.OrchestrationStateTableName}'), OBJECT_ID('{Settings.WorkItemTableName}')";

                await connection.OpenAsync();
                var reader = await command.ExecuteReaderAsync();

                if (await reader.ReadAsync())
                {
                    Assert.AreEqual(DBNull.Value, reader.GetValue(0), "Orchestration table found.");
                    Assert.AreEqual(DBNull.Value, reader.GetValue(1), "Work Item table found.");
                    return;
                }
            }

            Assert.Fail("No rows returned by query.");
        }
    }
}
