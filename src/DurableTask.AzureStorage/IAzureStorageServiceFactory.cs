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
    using Azure.Data.Tables;
    using Azure.Storage.Blobs;
    using Azure.Storage.Queues;

    /// <summary>
    /// Represents a factory that creates clients that communicate with different Azure Storage services.
    /// </summary>
    public interface IAzureStorageServiceFactory
    {
        /// <summary>
        /// Creates a <see cref="BlobServiceClient"/> for interacting with the Azure Blob service.
        /// </summary>
        /// <param name="configureOptions">An optional delegate for configuring the options for the client.</param>
        /// <returns>An instance of the <see cref="BlobServiceClient"/> class.</returns>
        BlobServiceClient CreateBlobServiceClient(Action<BlobClientOptions>? configureOptions = null);

        /// <summary>
        /// Creates a <see cref="QueueServiceClient"/> for interacting with the Azure Queue service.
        /// </summary>
        /// <param name="configureOptions">An optional delegate for configuring the options for the client.</param>
        /// <returns>An instance of the <see cref="QueueServiceClient"/> class.</returns>
        QueueServiceClient CreateQueueServiceClient(Action<QueueClientOptions>? configureOptions = null);

        /// <summary>
        /// Creates a <see cref="TableServiceClient"/> for interacting with the Azure Table service.
        /// </summary>
        /// <param name="configureOptions">An optional delegate for configuring the options for the client.</param>
        /// <returns>An instance of the <see cref="TableServiceClient"/> class.</returns>
        TableServiceClient CreateTableServiceClient(Action<TableClientOptions>? configureOptions = null);
    }
}
