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
namespace DurableTask.AzureStorage
{
    using System;
    using Azure.Core;
    using Azure.Data.Tables;
    using Azure.Storage.Blobs;
    using Azure.Storage.Queues;

    /// <summary>
    /// Represents a set of client providers for a single Azure Storage account.
    /// </summary>
    public sealed class StorageAccountDetails
    {
        /// <summary>
        /// Gets the provider for the Azure Blob Storage service client used to manage the task hub lease.
        /// </summary>
        /// <value>The <see cref="BlobServiceClient"/> instance.</value>
        public AzureStorageProvider<BlobServiceClient, BlobClientOptions>? BlobClientProvider { get; set; }

        /// <summary>
        /// Gets the provider for the Azure Queue Storage service client used to retrieve control and work item messages.
        /// </summary>
        /// <value>The <see cref="QueueServiceClient"/> instance.</value>
        public AzureStorageProvider<QueueServiceClient, QueueClientOptions>? QueueClientProvider { get; set; }

        /// <summary>
        /// Gets the provider for the Azure Table Storage service client used for tracking the
        /// progress of durable orchestrations and entity operations.
        /// </summary>
        /// <value>The <see cref="TableServiceClient"/> instance.</value>
        public AzureStorageProvider<TableServiceClient, TableClientOptions>? TableClientProvider { get; set; }

        /// <summary>
        /// Creates <see cref="StorageAccountDetails"/> based on the given account name.
        /// </summary>
        /// <param name="accountName">An Azure Storage account name.</param>
        /// <param name="tokenCredential">An optional token credential for accessing the services.</param>
        /// <returns>Storage account details whose connections are based on the given <paramref name="accountName"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="accountName"/> is <see langword="null"/> or consists entirely of white space characters.
        /// </exception>
        public static StorageAccountDetails FromAccountName(
            string accountName,
            TokenCredential? tokenCredential = null)
        {
            return new StorageAccountDetails
            {
                BlobClientProvider = AzureStorageProvider.ForBlobAccount(accountName, tokenCredential),
                QueueClientProvider = AzureStorageProvider.ForQueueAccount(accountName, tokenCredential),
                TableClientProvider = AzureStorageProvider.ForTableAccount(accountName, tokenCredential),
            };
        }

        /// <summary>
        /// Creates <see cref="StorageAccountDetails"/> based on the given connection string.
        /// </summary>
        /// <param name="connectionString">An Azure Storage connection string for the services.</param>
        /// <returns>Storage account details whose connections are based on the given <paramref name="connectionString"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="connectionString"/> is <see langword="null"/> or the empty string.
        /// </exception>
        public static StorageAccountDetails FromConnectionString(string connectionString)
        {
            return new StorageAccountDetails
            {
                BlobClientProvider = AzureStorageProvider.ForBlob(connectionString),
                QueueClientProvider = AzureStorageProvider.ForQueue(connectionString),
                TableClientProvider = AzureStorageProvider.ForTable(connectionString),
            };
        }

        /// <summary>
        /// Creates <see cref="StorageAccountDetails"/> based on the given service URIs.
        /// </summary>
        /// <param name="blobServiceUri">The URI for the Azure Blob Storage service.</param>
        /// <param name="queueServiceUri">The URI for the Azure Queue Storage service.</param>
        /// <param name="tableServiceUri">The URI for the Azure Table Storage service.</param>
        /// <param name="tokenCredential">An optional token credential for accessing the services.</param>
        /// <returns>Storage account details whose connections are based on the given URIs.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="blobServiceUri"/>, <paramref name="queueServiceUri"/>, or
        /// <paramref name="tableServiceUri"/> is <see langword="null"/>.
        /// </exception>
        public static StorageAccountDetails FromServiceUris(
            Uri blobServiceUri,
            Uri queueServiceUri,
            Uri tableServiceUri,
            TokenCredential? tokenCredential = null)
        {
            return new StorageAccountDetails
            {
                BlobClientProvider = AzureStorageProvider.ForBlob(blobServiceUri, tokenCredential),
                QueueClientProvider = AzureStorageProvider.ForQueue(queueServiceUri, tokenCredential),
                TableClientProvider = AzureStorageProvider.ForTable(tableServiceUri, tokenCredential),
            };
        }
    }
}
