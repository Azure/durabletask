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
#nullable enable
namespace DurableTask.AzureStorage.Tests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Core;
    using Azure.Data.Tables;
    using Azure.Storage.Blobs;
    using Azure.Storage.Queues;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class StorageServiceClientProviderTests
    {
        [DataTestMethod]
        [DataRow("blob", false)]
        [DataRow("blob", true)]
        [DataRow("queue", false)]
        [DataRow("queue", true)]
        [DataRow("table", false)]
        [DataRow("table", true)]
        public void TokenProvider_ExposesTheSuppliedCredential(string service, bool useAccountName)
        {
            // 1. Create a provider with an explicit credential and options.
            TestCredential credential = new TestCredential();
            Uri endpoint = new Uri($"https://account.{service}.core.windows.net");
            switch (service)
            {
                case "blob":
                    BlobClientOptions blobOptions = new BlobClientOptions();
                    IStorageServiceClientProvider<BlobServiceClient, BlobClientOptions> blobs = useAccountName
                        ? StorageServiceClientProvider.ForBlob("account", credential, blobOptions)
                        : StorageServiceClientProvider.ForBlob(endpoint, credential, blobOptions);
                    AssertProvider(blobs, blobOptions, credential);
                    break;
                case "queue":
                    QueueClientOptions queueOptions = new QueueClientOptions();
                    IStorageServiceClientProvider<QueueServiceClient, QueueClientOptions> queues = useAccountName
                        ? StorageServiceClientProvider.ForQueue("account", credential, queueOptions)
                        : StorageServiceClientProvider.ForQueue(endpoint, credential, queueOptions);
                    AssertProvider(queues, queueOptions, credential);
                    break;
                case "table":
                    TableClientOptions tableOptions = new TableClientOptions();
                    IStorageServiceClientProvider<TableServiceClient, TableClientOptions> tables = useAccountName
                        ? StorageServiceClientProvider.ForTable("account", credential, tableOptions)
                        : StorageServiceClientProvider.ForTable(endpoint, credential, tableOptions);
                    AssertProvider(tables, tableOptions, credential);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(service));
            }
        }

        [DataTestMethod]
        [DataRow("blob")]
        [DataRow("queue")]
        [DataRow("table")]
        public void ConnectionStringProvider_HasNoTokenCredential(string service)
        {
            // 1. Keep connection-string clients on their existing authentication path.
            const string connectionString = "UseDevelopmentStorage=true";
            switch (service)
            {
                case "blob":
                    BlobClientOptions blobOptions = new BlobClientOptions();
                    AssertProvider(StorageServiceClientProvider.ForBlob(connectionString, blobOptions), blobOptions, null);
                    break;
                case "queue":
                    QueueClientOptions queueOptions = new QueueClientOptions();
                    AssertProvider(StorageServiceClientProvider.ForQueue(connectionString, queueOptions), queueOptions, null);
                    break;
                case "table":
                    TableClientOptions tableOptions = new TableClientOptions();
                    AssertProvider(StorageServiceClientProvider.ForTable(connectionString, tableOptions), tableOptions, null);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(service));
            }
        }

        [TestMethod]
        public void CustomProvider_DoesNotRequireTheOptionalInterface()
        {
            // 1. Implement only the existing client-provider contract.
            IStorageServiceClientProvider<object, TableClientOptions> provider = new CustomProvider();

            // 2. Client creation still works without exposing a credential.
            Assert.IsFalse(provider is IStorageTokenCredentialProvider);
            Assert.IsNotNull(provider.CreateClient(provider.CreateOptions()));
        }

        static void AssertProvider<TClient, TOptions>(
            IStorageServiceClientProvider<TClient, TOptions> provider, TOptions options, TokenCredential? credential)
            where TOptions : ClientOptions
        {
            // 2. Keep the same credential and options, and continue creating clients.
            Assert.IsInstanceOfType<IStorageTokenCredentialProvider>(provider);
            Assert.AreSame(credential, ((IStorageTokenCredentialProvider)provider).TokenCredential);
            Assert.AreSame(options, provider.CreateOptions());
            Assert.IsNotNull(provider.CreateClient(options));
        }

        sealed class TestCredential : TokenCredential
        {
            public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
                throw new InvalidOperationException("Creating a provider must not request a token.");

            public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
                throw new InvalidOperationException("Creating a provider must not request a token.");
        }

        sealed class CustomProvider : IStorageServiceClientProvider<object, TableClientOptions>
        {
            public object CreateClient(TableClientOptions options) => new object();
            public TableClientOptions CreateOptions() => new TableClientOptions();
        }
    }
}
