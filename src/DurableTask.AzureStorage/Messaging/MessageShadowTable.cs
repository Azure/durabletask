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
namespace DurableTask.AzureStorage.Messaging
{
    using System;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Data.Tables;
    using DurableTask.AzureStorage.Storage;

    /// <summary>
    /// Stores queue messages that must remain discoverable during a live migration.
    /// </summary>
    class MessageShadowTable
    {
        internal const string MessageDataPropertyName = "MessageData";

        readonly Table table;

        public MessageShadowTable(AzureStorageClient azureStorageClient)
        {
            this.table = azureStorageClient.GetTableReference(azureStorageClient.Settings.MessageShadowTableName);
        }

        public string Name => this.table.Name;

        public Task<bool> CreateIfNotExistsAsync(CancellationToken cancellationToken = default)
        {
            return this.table.CreateIfNotExistsAsync(cancellationToken);
        }

        public Task<bool> DeleteIfExistsAsync(CancellationToken cancellationToken = default)
        {
            return this.table.DeleteIfExistsAsync(cancellationToken);
        }

        public Task AddMessageAsync(
            MessageData message,
            string serializedMessage,
            CancellationToken cancellationToken = default)
        {
            if (!message.ShadowMessageId.HasValue)
            {
                throw new ArgumentException("The message must have a shadow message ID.", nameof(message));
            }

            var entity = new TableEntity(
                message.QueueName,
                GetRowKey(message.ShadowMessageId.Value))
            {
                [MessageDataPropertyName] = Encoding.UTF8.GetBytes(serializedMessage),
            };

            return this.table.InsertEntityAsync(entity, cancellationToken);
        }

        public async Task DeleteMessageAsync(
            string queueName,
            Guid shadowMessageId,
            CancellationToken cancellationToken = default)
        {
            var entity = new TableEntity(queueName, GetRowKey(shadowMessageId));

            try
            {
                await this.table.DeleteEntityAsync(entity, ETag.All, cancellationToken);
            }
            catch (DurableTaskStorageException e) when (e.HttpStatusCode == 404)
            {
                // A duplicate delivery may have already removed this row.
            }
        }

        internal static string GetRowKey(Guid shadowMessageId)
        {
            return shadowMessageId.ToString("N");
        }
    }
}
