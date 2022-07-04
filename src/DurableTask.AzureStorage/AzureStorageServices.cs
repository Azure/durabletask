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

namespace DurableTask.AzureStorage
{
    using System;
    using System.Globalization;
    using Azure.Core;
    using Azure.Data.Tables;
    using Azure.Storage.Blobs;
    using Azure.Storage.Queues;

    /// <summary>
    /// Represents the set of Azure Storage services used by the Durable Task framework.
    /// </summary>
    public sealed class AzureStorageServices
    {
        /// <summary>
        /// Gets or sets the client used to manage the task hub lease.
        /// </summary>
        /// <value>The <see cref="BlobServiceClient"/> instance.</value>
        public BlobServiceClient Blob { get; set; }

        /// <summary>
        /// Gets or sets the client used to retrieve control and work item messages.
        /// </summary>
        /// <value>The <see cref="QueueServiceClient"/> instance.</value>
        public QueueServiceClient Queue { get; set; }

        /// <summary>
        /// Gets or sets the client used for tracking the progress of durable orchestrations and entity operations.
        /// </summary>
        /// <value>The <see cref="TableServiceClient"/> instance.</value>
        public TableServiceClient Table { get; set; }

        /// <summary>
        /// Creates a set of <see cref="AzureStorageServices"/> from an account name.
        /// </summary>
        /// <param name="accountName">An Azure Storage account name.</param>
        /// <param name="tokenCredential">An optional token credential for accessing the services.</param>
        /// <returns>A set of service clients whose connections are based on the given <paramref name="accountName"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="accountName"/> is <see langword="null"/> or consists entirely of white space characters.
        /// </exception>
        public static AzureStorageServices FromAccountName(string accountName, TokenCredential tokenCredential = null) =>
            tokenCredential != null
                ? new AzureStorageServices
                {
                    Blob = new BlobServiceClient(CreateDefaultServiceUri(accountName, "blob"), tokenCredential),
                    Queue = new QueueServiceClient(CreateDefaultServiceUri(accountName, "queue"), tokenCredential),
                    Table = new TableServiceClient(CreateDefaultServiceUri(accountName, "table"), tokenCredential),
                }
                : new AzureStorageServices
                {
                    Blob = new BlobServiceClient(CreateDefaultServiceUri(accountName, "blob")),
                    Queue = new QueueServiceClient(CreateDefaultServiceUri(accountName, "queue")),
                    Table = new TableServiceClient(CreateDefaultServiceUri(accountName, "table")),
                };

        /// <summary>
        /// Creates a set of <see cref="AzureStorageServices"/> from a single <paramref name="connectionString"/>.
        /// </summary>
        /// <param name="connectionString">An Azure Storage connection string for blob, queue, and table services.</param>
        /// <returns>A set of service clients whose connections are based on the given <paramref name="connectionString"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="connectionString"/> is <see langword="null"/> or the empty string.
        /// </exception>
        public static AzureStorageServices FromConnectionString(string connectionString) =>
            new AzureStorageServices
            {
                Blob = new BlobServiceClient(connectionString),
                Queue = new QueueServiceClient(connectionString),
                Table = new TableServiceClient(connectionString),
            };

        private static Uri CreateDefaultServiceUri(string accountName, string service)
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
