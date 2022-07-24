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
    using System.Globalization;
    using Azure.Core;
    using Azure.Data.Tables;
    using Azure.Storage.Blobs;
    using Azure.Storage.Queues;

    /// <summary>
    /// Represents a <see langword="static"/> set of methods for easily creating instances of type
    /// <see cref="IAzureStorageProvider{TClient, TClientOptions}"/>.
    /// </summary>
    public static class AzureStorageProvider
    {
        #region Blob

        /// <summary>
        /// Creates an <see cref="IAzureStorageProvider{TClient, TClientOptions}"/> for the Azure Blob Storage service.
        /// </summary>
        /// <param name="accountName">An Azure Storage account name.</param>
        /// <param name="tokenCredential">An optional token credential for accessing the service.</param>
        /// <param name="options">An optional set of client options.</param>
        /// <returns>An Azure Blob Storage service client whose connection is based on the given <paramref name="accountName"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="accountName"/> is <see langword="null"/> or consists entirely of white space characters.
        /// </exception>
        public static IAzureStorageProvider<BlobServiceClient, BlobClientOptions> ForBlobAccount(
            string accountName,
            TokenCredential? tokenCredential = null,
            BlobClientOptions? options = null)
        {
            return ForBlob(CreateDefaultServiceUri(accountName, "blob"), tokenCredential, options);
        }

        /// <summary>
        /// Creates an <see cref="IAzureStorageProvider{TClient, TClientOptions}"/> for the Azure Blob Storage service.
        /// </summary>
        /// <param name="connectionString">An Azure Storage connection string for the blob service.</param>
        /// <param name="options">An optional set of client options.</param>
        /// <returns>An Azure Blob Storage service client whose connection is based on the given <paramref name="connectionString"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="connectionString"/> is <see langword="null"/> or the empty string.
        /// </exception>
        public static IAzureStorageProvider<BlobServiceClient, BlobClientOptions> ForBlob(string connectionString, BlobClientOptions? options = null)
        {
            return new DefaultAzureStorageProvider<BlobServiceClient, BlobClientOptions>(
                o => new BlobServiceClient(connectionString, o),
                options ?? new BlobClientOptions());
        }

        /// <summary>
        /// Creates an <see cref="IAzureStorageProvider{TClient, TClientOptions}"/> for the Azure Blob Storage service.
        /// </summary>
        /// <param name="serviceUri">An Azure Blob Storage service URI.</param>
        /// <param name="tokenCredential">An optional token credential for accessing the service.</param>
        /// <param name="options">An optional set of client options.</param>
        /// <returns>An Azure Blob Storage service client whose connection is based on the given <paramref name="serviceUri"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="serviceUri"/> is <see langword="null"/>.</exception>
        public static IAzureStorageProvider<BlobServiceClient, BlobClientOptions> ForBlob(
            Uri serviceUri,
            TokenCredential? tokenCredential = null,
            BlobClientOptions? options = null)
        {
            return new DefaultAzureStorageProvider<BlobServiceClient, BlobClientOptions>(
                o => tokenCredential != null ? new BlobServiceClient(serviceUri, tokenCredential, o) : new BlobServiceClient(serviceUri, o),
                options ?? new BlobClientOptions());
        }

        #endregion

        #region Queue

        /// <summary>
        /// Creates an <see cref="IAzureStorageProvider{TClient, TClientOptions}"/> for the Azure Queue Storage service.
        /// </summary>
        /// <param name="accountName">An Azure Storage account name.</param>
        /// <param name="tokenCredential">An optional token credential for accessing the service.</param>
        /// <param name="options">An optional set of client options.</param>
        /// <returns>An Azure Queue Storage service client whose connection is based on the given <paramref name="accountName"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="accountName"/> is <see langword="null"/> or consists entirely of white space characters.
        /// </exception>
        public static IAzureStorageProvider<QueueServiceClient, QueueClientOptions> ForQueueAccount(
            string accountName,
            TokenCredential? tokenCredential = null,
            QueueClientOptions? options = null)
        {
            return ForQueue(CreateDefaultServiceUri(accountName, "queue"), tokenCredential, options);
        }

        /// <summary>
        /// Creates an <see cref="IAzureStorageProvider{TClient, TClientOptions}"/> for the Azure Queue Storage service.
        /// </summary>
        /// <param name="connectionString">An Azure Storage connection string for the queue service.</param>
        /// <param name="options">An optional set of client options.</param>
        /// <returns>An Azure Queue Storage service client whose connection is based on the given <paramref name="connectionString"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="connectionString"/> is <see langword="null"/> or the empty string.
        /// </exception>
        public static IAzureStorageProvider<QueueServiceClient, QueueClientOptions> ForQueue(string connectionString, QueueClientOptions? options = null)
        {
            return new DefaultAzureStorageProvider<QueueServiceClient, QueueClientOptions>(
                o => new QueueServiceClient(connectionString, o),
                options ?? new QueueClientOptions());
        }

        /// <summary>
        /// Creates an <see cref="IAzureStorageProvider{TClient, TClientOptions}"/> for the Azure Queue Storage service.
        /// </summary>
        /// <param name="serviceUri">An Azure Queue Storage service URI.</param>
        /// <param name="tokenCredential">An optional token credential for accessing the service.</param>
        /// <param name="options">An optional set of client options.</param>
        /// <returns>An Azure Queue Storage service client whose connection is based on the given <paramref name="serviceUri"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="serviceUri"/> is <see langword="null"/>.</exception>
        public static IAzureStorageProvider<QueueServiceClient, QueueClientOptions> ForQueue(
            Uri serviceUri,
            TokenCredential? tokenCredential = null,
            QueueClientOptions? options = null)
        {
            return new DefaultAzureStorageProvider<QueueServiceClient, QueueClientOptions>(
                o => tokenCredential != null ? new QueueServiceClient(serviceUri, tokenCredential, o) : new QueueServiceClient(serviceUri, o),
                options ?? new QueueClientOptions());
        }

        #endregion

        #region Table

        /// <summary>
        /// Creates an <see cref="IAzureStorageProvider{TClient, TClientOptions}"/> for the Azure Table Storage service.
        /// </summary>
        /// <param name="accountName">An Azure Storage account name.</param>
        /// <param name="tokenCredential">An optional token credential for accessing the service.</param>
        /// <param name="options">An optional set of client options.</param>
        /// <returns>An Azure Table Storage service client whose connection is based on the given <paramref name="accountName"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="accountName"/> is <see langword="null"/> or consists entirely of white space characters.
        /// </exception>
        public static IAzureStorageProvider<TableServiceClient, TableClientOptions> ForTableAccount(
            string accountName,
            TokenCredential? tokenCredential = null,
            TableClientOptions? options = null)
        {
            return ForTable(CreateDefaultServiceUri(accountName, "table"), tokenCredential, options);
        }

        /// <summary>
        /// Creates an <see cref="IAzureStorageProvider{TClient, TClientOptions}"/> for the Azure Table Storage service.
        /// </summary>
        /// <param name="connectionString">An Azure Storage connection string for the table service.</param>
        /// <param name="options">An optional set of client options.</param>
        /// <returns>An Azure Table Storage service client whose connection is based on the given <paramref name="connectionString"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="connectionString"/> is <see langword="null"/> or the empty string.
        /// </exception>
        public static IAzureStorageProvider<TableServiceClient, TableClientOptions> ForTable(string connectionString, TableClientOptions? options = null)
        {
            return new DefaultAzureStorageProvider<TableServiceClient, TableClientOptions>(
                o => new TableServiceClient(connectionString, o),
                options ?? new TableClientOptions());
        }

        /// <summary>
        /// Creates an <see cref="IAzureStorageProvider{TClient, TClientOptions}"/> for the Azure Table Storage service.
        /// </summary>
        /// <param name="serviceUri">An Azure Table Storage service URI.</param>
        /// <param name="tokenCredential">An optional token credential for accessing the service.</param>
        /// <param name="options">An optional set of client options.</param>
        /// <returns>An Azure Table Storage service client whose connection is based on the given <paramref name="serviceUri"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="serviceUri"/> is <see langword="null"/>.</exception>
        public static IAzureStorageProvider<TableServiceClient, TableClientOptions> ForTable(
            Uri serviceUri,
            TokenCredential? tokenCredential = null,
            TableClientOptions? options = null)
        {
            return new DefaultAzureStorageProvider<TableServiceClient, TableClientOptions>(
                o => tokenCredential != null ? new TableServiceClient(serviceUri, tokenCredential, o) : new TableServiceClient(serviceUri, o),
                options ?? new TableClientOptions());
        }

        #endregion

        static Uri CreateDefaultServiceUri(string accountName, string service)
        {
            if (string.IsNullOrWhiteSpace(accountName))
            {
                throw new ArgumentException("No Azure Storage account name specified.", nameof(accountName));
            }

            if (string.IsNullOrWhiteSpace(service))
            {
                throw new ArgumentException("No Azure Storage service specified.", nameof(service));
            }

            return new Uri(
                string.Format(CultureInfo.InvariantCulture, "https://{0}.{1}.core.windows.net/", accountName, service),
                UriKind.Absolute);
        }
    }
}
