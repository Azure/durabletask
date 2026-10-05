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
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Data.Tables;

    /// <summary>
    /// Coordinates the migration of a task hub. This control table must not be deleted during migration metadata cleanup:
    /// its ending flag permanently disables any writes from the backend.
    /// </summary>
    public sealed class AzureStorageMigration
    {
        const string MigrationEndingPropertyName = "IsDraining";

        readonly TableClient table;

        /// <summary>Creates a migration drain coordinator using the tracking account's authenticated Table service client.</summary>
        /// <param name="tableServiceClient">An authenticated client independent of workload SAS credentials.</param>
        /// <param name="taskHubName">The source task hub name.</param>
        public AzureStorageMigration(TableServiceClient tableServiceClient, string taskHubName)
        {
            if (tableServiceClient == null)
            {
                throw new ArgumentNullException(nameof(tableServiceClient));
            }
            if (string.IsNullOrWhiteSpace(taskHubName))
            {
                throw new ArgumentException("A task hub name is required.", nameof(taskHubName));
            }
            this.table = tableServiceClient.GetTableClient(taskHubName + "MigrationControl");
        }

        internal async Task CreateIfNotExistsAsync(CancellationToken cancellationToken)
        {
            await this.table.CreateIfNotExistsAsync(cancellationToken);
            try
            {
                await this.table.AddEntityAsync(new TableEntity(string.Empty, string.Empty)
                {
                    [MigrationEndingPropertyName] = false,
                    // No token has been issued yet. The first host to renew the expiry time will set this to a future value.
                    [nameof(Gate.AccessExpiresAt)] = DateTimeOffset.FromUnixTimeSeconds(0),
                }, cancellationToken);
            }
            catch (RequestFailedException e) when (e.Status == 409)
            {
                // Another host may have created or drained the row. Leave it unchanged.
            }
        }

        internal async Task<Gate?> ReadAsync(CancellationToken cancellationToken)
        {
            try
            {
                Response<TableEntity> response = await this.table.GetEntityAsync<TableEntity>(string.Empty, string.Empty, cancellationToken: cancellationToken);
                TableEntity row = response.Value;
                return new Gate
                {
                    IsMigrationEnding = row.GetBoolean(MigrationEndingPropertyName) ?? throw new InvalidOperationException($"Migration gate is missing {MigrationEndingPropertyName}."),
                    AccessExpiresAt = row.GetDateTimeOffset(nameof(Gate.AccessExpiresAt)) ?? throw new InvalidOperationException("Migration gate is missing AccessExpiresAt."),
                    ETag = row.ETag,
                    // Use the storage response clock, not the source host's wall clock
                    ServerTime = response.GetRawResponse().Headers.Date ?? throw new InvalidOperationException("Migration gate response is missing the Storage Date header."),
                };
            }
            catch (RequestFailedException e) when (e.Status == 404 && (e.ErrorCode == "ResourceNotFound" || e.ErrorCode == "TableNotFound"))
            {
                return null;
            }
        }

        internal async Task<bool> TryUpdateAsync(Gate gate, CancellationToken cancellationToken)
        {
            try
            {
                await this.table.UpdateEntityAsync(new TableEntity(string.Empty, string.Empty)
                {
                    [MigrationEndingPropertyName] = gate.IsMigrationEnding,
                    [nameof(Gate.AccessExpiresAt)] = gate.AccessExpiresAt,
                }, gate.ETag, TableUpdateMode.Replace, cancellationToken);
                return true;
            }
            // Another host may have updated the gate. The caller can retry with a fresh read.
            catch (RequestFailedException e) when (e.Status == 412)
            {
                return false;
            }
        }

        internal sealed class Gate
        {
            public bool IsMigrationEnding { get; set; }
            public DateTimeOffset AccessExpiresAt { get; set; }
            public ETag ETag { get; set; }
            public DateTimeOffset ServerTime { get; set; }
        }
    }
}
