using Docker.DotNet;
using Docker.DotNet.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.SqlServer.Tests
{
    [TestClass]
    public class DatabaseInitialization
    {
        //Container will fail to start up if the password provided is not a "strong" password
        private const string saPassword = "S3cur3P@$$w0rd";

        private static readonly AppSettings appSettings;

        static DatabaseInitialization()
        {
            appSettings = new AppSettings();

            new ConfigurationBuilder()
                .SetBasePath(Environment.CurrentDirectory)
                .AddJsonFile("appsettings.json", optional: true)
                .Build()
                .Bind(appSettings);
        }

        [AssemblyInitialize]
        public static void Initialize(TestContext context)
        {
            //perform cleanup to return system to a known state
            EnsureSqlServerContainerIsRemoved().Wait();

            using (var client = new DockerClientConfiguration(appSettings.DockerEndpoint).CreateClient())
            {
                client.Images.CreateImageAsync(new ImagesCreateParameters { FromImage = appSettings.SqlContainer.Image, Tag = appSettings.SqlContainer.Tag }, null, new NoOpProgress()).Wait();

                var response = client.Containers.CreateContainerAsync(new CreateContainerParameters
                {
                    Name = "DurableTask_SQLServer_Test",
                    Image = $"{appSettings.SqlContainer.Image}:{appSettings.SqlContainer.Tag}",
                    Env = new[] { "ACCEPT_EULA=Y", $"SA_PASSWORD={saPassword}" },
                    HostConfig = new HostConfig
                    {
                        PortBindings = new Dictionary<string, IList<PortBinding>>
                    {
                        {"1433/tcp", new[]{new PortBinding { HostPort = "1433"} } }
                    }
                    }
                }).Result;

                var hasStarted = client.Containers.StartContainerAsync("DurableTask_SQLServer_Test", new ContainerStartParameters()).Result;

                if (hasStarted == false) throw new Exception("SQL Server container failed to start.");
            }

            EnsureDatabaseConnectivity();

            using (var connection = GetMasterDatabaseConnection())
            using (var command = connection.CreateCommand())
            {
                var databaseName = GetConnectionStringBuilder().InitialCatalog;
                command.CommandText = $"CREATE DATABASE [{databaseName}]";
                
                connection.Open();
                command.ExecuteNonQuery();
            }
        }
        [AssemblyCleanup]
        public static void Cleanup()
        {
            EnsureSqlServerContainerIsRemoved().Wait();
        }

        public static async Task EnsureSqlServerContainerIsRemoved()
        {
            try
            {
                using (var client = new DockerClientConfiguration(appSettings.DockerEndpoint).CreateClient())
                {
                    await client.Containers.StopContainerAsync("DurableTask_SQLServer_Test", new ContainerStopParameters());

                    await client.Containers.RemoveContainerAsync("DurableTask_SQLServer_Test", new ContainerRemoveParameters());
                }
            }
            catch (DockerContainerNotFoundException) { }
        }



        private static void EnsureDatabaseConnectivity()
        {
            using (var connection = GetMasterDatabaseConnection())
            {
                const int maxAttempts = 10;
                for (var i = 0; i < maxAttempts; i++)
                {
                    if (i > 0) Thread.Sleep(6000);

                    try
                    {
                        connection.Open();
                        return;
                    }
                    catch (SqlException) { }
                }
            }

            throw new Exception("Unable to open connection to SQL Server");
        }

        private static SqlConnection GetMasterDatabaseConnection()
        {
            var builder = GetConnectionStringBuilder();
            builder.InitialCatalog = "master";

            return new SqlConnection(builder.ToString());
        }

        public static SqlConnection GetDatabaseConnection()
        {
            return new SqlConnection(GetConnectionStringBuilder().ToString());
        }

        private static SqlConnectionStringBuilder GetConnectionStringBuilder() =>
            new SqlConnectionStringBuilder(appSettings.ConnectionStrings["SqlServer"]) { Password = saPassword };

        public class NoOpProgress : IProgress<JSONMessage>
        {
            public void Report(JSONMessage value) { }
        }
    }
}
