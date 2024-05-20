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
    /// <see cref="IStorageServiceClientProvider{TClient, TClientOptions}"/>.
    /// </summary>
    public static class StorageServiceClientProvider
    {
        #region Blob

        /// <summary>
        /// Creates an <see cref="IStorageServiceClientProvider{TClient, TClientOptions}"/> for the Azure Blob Storage service.
        /// </summary>
        /// <param name="connectionString">An Azure Storage connection string for the blob service.</param>
        /// <param name="options">An optional set of client options.</param>
        /// <returns>An Azure Blob Storage service client whose connection is based on the given <paramref name="connectionString"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="connectionString"/> is <see langword="null"/> or consists entirely of white space characters.
        /// </exception>
        public static IStorageServiceClientProvider<BlobServiceClient, BlobClientOptions> ForBlob(string connectionString, BlobClientOptions? options = null)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            return new DefaultStorageServiceClientProvider<BlobServiceClient, BlobClientOptions>(
                o => new BlobServiceClient(connectionString, o),
                options ?? new BlobClientOptions());
        }

        /// <summary>
        /// Creates an <see cref="IStorageServiceClientProvider{TClient, TClientOptions}"/> for the Azure Blob Storage service.
        /// </summary>
        /// <param name="accountName">An Azure Storage account name.</param>
        /// <param name="tokenCredential">A token credential for accessing the service.</param>
        /// <param name="options">An optional set of client options.</param>
        /// <returns>An Azure Blob Storage service client whose connection is based on the given <paramref name="accountName"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <para>
        /// <paramref name="accountName"/> is <see langword="null"/> or consists entirely of white space characters.
        /// </para>
        /// <para>-or-</para>
        /// <para><paramref name="tokenCredential"/> is <see langword="null"/>.</para>
        /// </exception>
        public static IStorageServiceClientProvider<BlobServiceClient, BlobClientOptions> ForBlob(
            string accountName,
            TokenCredential tokenCredential,
            BlobClientOptions? options = null)
        {
            return ForBlob(CreateDefaultServiceUri(accountName, "blob"), tokenCredential, options);
        }

        /// <summary>
        /// Creates an <see cref="IStorageServiceClientProvider{TClient, TClientOptions}"/> for the Azure Blob Storage service.
        /// </summary>
        /// <param name="serviceUri">An Azure Blob Storage service URI.</param>
        /// <param name="tokenCredential">A token credential for accessing the service.</param>
        /// <param name="options">An optional set of client options.</param>
        /// <returns>An Azure Blob Storage service client whose connection is based on the given <paramref name="serviceUri"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="serviceUri"/> or <paramref name="tokenCredential"/> is <see langword="null"/>.
        /// </exception>
        public static IStorageServiceClientProvider<BlobServiceClient, BlobClientOptions> ForBlob(
            Uri serviceUri,
            TokenCredential tokenCredential,
            BlobClientOptions? options = null)
        {
            if (tokenCredential == null)
            {
                throw new ArgumentNullException(nameof(tokenCredential));
            }

            return new DefaultStorageServiceClientProvider<BlobServiceClient, BlobClientOptions>(
                o => new BlobServiceClient(serviceUri, tokenCredential, o),
                options ?? new BlobClientOptions());
        }

        #endregion

        #region Queue

        /// <summary>
        /// Creates an <see cref="IStorageServiceClientProvider{TClient, TClientOptions}"/> for the Azure Queue Storage service.
        /// </summary>
        /// <param name="connectionString">An Azure Storage connection string for the queue service.</param>
        /// <param name="options">An optional set of client options.</param>
        /// <returns>An Azure Queue Storage service client whose connection is based on the given <paramref name="connectionString"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="connectionString"/> is <see langword="null"/> or consists entirely of white space characters.
        /// </exception>
        public static IStorageServiceClientProvider<QueueServiceClient, QueueClientOptions> ForQueue(string connectionString, QueueClientOptions? options = null)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            return new DefaultStorageServiceClientProvider<QueueServiceClient, QueueClientOptions>(
                o => new QueueServiceClient(connectionString, o),
                options ?? new QueueClientOptions());
        }

        /// <summary>
        /// Creates an <see cref="IStorageServiceClientProvider{TClient, TClientOptions}"/> for the Azure Queue Storage service.
        /// </summary>
        /// <param name="accountName">An Azure Storage account name.</param>
        /// <param name="tokenCredential">A token credential for accessing the service.</param>
        /// <param name="options">An optional set of client options.</param>
        /// <returns>An Azure Queue Storage service client whose connection is based on the given <paramref name="accountName"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <para>
        /// <paramref name="accountName"/> is <see langword="null"/> or consists entirely of white space characters.
        /// </para>
        /// <para>-or-</para>
        /// <para><paramref name="tokenCredential"/> is <see langword="null"/>.</para>
        /// </exception>
        public static IStorageServiceClientProvider<QueueServiceClient, QueueClientOptions> ForQueue(
            string accountName,
            TokenCredential tokenCredential,
            QueueClientOptions? options = null)
        {
            return ForQueue(CreateDefaultServiceUri(accountName, "queue"), tokenCredential, options);
        }

        /// <summary>
        /// Creates an <see cref="IStorageServiceClientProvider{TClient, TClientOptions}"/> for the Azure Queue Storage service.
        /// </summary>
        /// <param name="serviceUri">An Azure Queue Storage service URI.</param>
        /// <param name="tokenCredential">A token credential for accessing the service.</param>
        /// <param name="options">An optional set of client options.</param>
        /// <returns>An Azure Queue Storage service client whose connection is based on the given <paramref name="serviceUri"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="serviceUri"/> or <paramref name="tokenCredential"/> is <see langword="null"/>.
        /// </exception>
        public static IStorageServiceClientProvider<QueueServiceClient, QueueClientOptions> ForQueue(
            Uri serviceUri,
            TokenCredential tokenCredential,
            QueueClientOptions? options = null)
        {
            if (tokenCredential == null)
            {
                throw new ArgumentNullException(nameof(tokenCredential));
            }

            return new DefaultStorageServiceClientProvider<QueueServiceClient, QueueClientOptions>(
                o => new QueueServiceClient(serviceUri, tokenCredential, o),
                options ?? new QueueClientOptions());
        }

        #endregion

        #region Table

        /// <summary>
        /// Creates an <see cref="IStorageServiceClientProvider{TClient, TClientOptions}"/> for the Azure Table Storage service.
        /// </summary>
        /// <param name="connectionString">An Azure Storage connection string for the table service.</param>
        /// <param name="options">An optional set of client options.</param>
        /// <returns>An Azure Table Storage service client whose connection is based on the given <paramref name="connectionString"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="connectionString"/> is <see langword="null"/> or consists entirely of white space characters.
        /// </exception>
        public static IStorageServiceClientProvider<TableServiceClient, TableClientOptions> ForTable(string connectionString, TableClientOptions? options = null)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            return new DefaultStorageServiceClientProvider<TableServiceClient, TableClientOptions>(
                o => new TableServiceClient(connectionString, o),
                options ?? new TableClientOptions());
        }

        /// <summary>
        /// Creates an <see cref="IStorageServiceClientProvider{TClient, TClientOptions}"/> for the Azure Table Storage service.
        /// </summary>
        /// <param name="accountName">An Azure Storage account name.</param>
        /// <param name="tokenCredential">A token credential for accessing the service.</param>
        /// <param name="options">An optional set of client options.</param>
        /// <returns>An Azure Table Storage service client whose connection is based on the given <paramref name="accountName"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <para>
        /// <paramref name="accountName"/> is <see langword="null"/> or consists entirely of white space characters.
        /// </para>
        /// <para>-or-</para>
        /// <para><paramref name="tokenCredential"/> is <see langword="null"/>.</para>
        /// </exception>
        public static IStorageServiceClientProvider<TableServiceClient, TableClientOptions> ForTable(
            string accountName,
            TokenCredential tokenCredential,
            TableClientOptions? options = null)
        {
            return ForTable(CreateDefaultServiceUri(accountName, "table"), tokenCredential, options);
        }

        /// <summary>
        /// Creates an <see cref="IStorageServiceClientProvider{TClient, TClientOptions}"/> for the Azure Table Storage service.
        /// </summary>
        /// <param name="serviceUri">An Azure Table Storage service URI.</param>
        /// <param name="tokenCredential">A token credential for accessing the service.</param>
        /// <param name="options">An optional set of client options.</param>
        /// <returns>An Azure Table Storage service client whose connection is based on the given <paramref name="serviceUri"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="serviceUri"/> or <paramref name="tokenCredential"/> is <see langword="null"/>.
        /// </exception>
        public static IStorageServiceClientProvider<TableServiceClient, TableClientOptions> ForTable(
            Uri serviceUri,
            TokenCredential tokenCredential,
            TableClientOptions? options = null)
        {
            if (tokenCredential == null)
            {
                throw new ArgumentNullException(nameof(tokenCredential));
            }

            return new DefaultStorageServiceClientProvider<TableServiceClient, TableClientOptions>(
                o => new TableServiceClient(serviceUri, tokenCredential, o),
                options ?? new TableClientOptions());
        }

        #endregion

        static Uri CreateDefaultServiceUri(string accountName, string service)
        {
            if (string.IsNullOrWhiteSpace(accountName))
            {
                throw new ArgumentNullException(nameof(accountName));
            }

            if (string.IsNullOrWhiteSpace(service))
            {
                throw new ArgumentNullException(nameof(service));
            }

            return new Uri(
                string.Format(CultureInfo.InvariantCulture, "https://{0}.{1}.core.windows.net/", accountName, service),
                UriKind.Absolute);
        }
    }
}
