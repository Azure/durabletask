
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

    /// <summary>
    /// Represents a client provider for the services used to track the execution of Durable Tasks.
    /// </summary>
    public class TrackingServiceClientProvider
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TrackingServiceClientProvider"/> class that returns
        /// service clients using the given <paramref name="connectionString"/>.
        /// </summary>
        /// <param name="connectionString">An Azure Storage connection string.</param>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="connectionString"/> is <see langword="null"/> or consists entirely of white space characters.
        /// </exception>
        public TrackingServiceClientProvider(string connectionString)
            : this(
                  StorageServiceClientProvider.ForBlob(connectionString),
                  StorageServiceClientProvider.ForTable(connectionString))
        { }

        /// <summary>
        /// Initializes a new instance of the <see cref="TrackingServiceClientProvider"/> class that returns
        /// service clients using the given <paramref name="accountName"/> and credential.
        /// </summary>
        /// <param name="accountName">An Azure Storage account name.</param>
        /// <param name="tokenCredential">A token credential for accessing the service.</param>
        /// <returns>An Azure Blob Storage service client whose connection is based on the given <paramref name="accountName"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <para>
        /// <paramref name="accountName"/> is <see langword="null"/> or consists entirely of white space characters.
        /// </para>
        /// <para>-or-</para>
        /// <para><paramref name="tokenCredential"/> is <see langword="null"/>.</para>
        /// </exception>
        public TrackingServiceClientProvider(string accountName, TokenCredential tokenCredential)
            : this(
                  StorageServiceClientProvider.ForBlob(accountName, tokenCredential),
                  StorageServiceClientProvider.ForTable(accountName, tokenCredential))
        { }

        /// <summary>
        /// Initializes a new instance of the <see cref="TrackingServiceClientProvider"/> class that returns
        /// service clients using the given service URIs and credential.
        /// </summary>
        /// <param name="blobServiceUri">An Azure Blob Storage service URI.</param>
        /// <param name="tableServiceUri">An Azure Table Storage service URI.</param>
        /// <param name="tokenCredential">A token credential for accessing the storage services.</param>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="blobServiceUri"/>, <paramref name="tableServiceUri"/>,
        /// or <paramref name="tokenCredential"/> is <see langword="null"/>.
        /// </exception>
        public TrackingServiceClientProvider(Uri blobServiceUri, Uri tableServiceUri, TokenCredential tokenCredential)
            : this(
                  StorageServiceClientProvider.ForBlob(blobServiceUri, tokenCredential),
                  StorageServiceClientProvider.ForTable(tableServiceUri, tokenCredential))
        { }

        /// <summary>
        /// Initializes a new instance of the <see cref="TrackingServiceClientProvider"/> class that returns
        /// service clients using the given client providers.
        /// </summary>
        /// <param name="blob">An Azure Blob Storage service client provider.</param>
        /// <param name="table">An Azure Table Storage service client provider.</param>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="blob"/> or <paramref name="table"/> is <see langword="null"/>.
        /// </exception>
        public TrackingServiceClientProvider(
            IStorageServiceClientProvider<BlobServiceClient, BlobClientOptions> blob,
            IStorageServiceClientProvider<TableServiceClient, TableClientOptions> table)
        {
            this.Blob = blob ?? throw new ArgumentNullException(nameof(blob));
            this.Table = table ?? throw new ArgumentNullException(nameof(table));
        }

        /// <summary>
        /// Gets the client provider for Azure Blob Storage.
        /// </summary>
        public IStorageServiceClientProvider<BlobServiceClient, BlobClientOptions> Blob { get; }

        /// <summary>
        /// Gets the client provider for Azure Table Storage.
        /// </summary>
        public IStorageServiceClientProvider<TableServiceClient, TableClientOptions> Table { get; }
    }
}
