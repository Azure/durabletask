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


namespace DurableTask.SqlServer.Tracking
{
    using DurableTask.Core;
    using DurableTask.Core.History;
    using DurableTask.Core.Serializing;
    using DurableTask.Core.Tracking;
    using DurableTask.SqlServer.Internal;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text.RegularExpressions;
    using System.Threading.Tasks;

    /// <summary>
    /// SQL Server Instance store provider to allow storage and lookup for orchestration state event history with query support
    /// </summary>
    public class SqlServerInstanceStore : IOrchestrationServiceInstanceStore
    {
        private readonly SqlServerInstanceStoreSettings settings;

        /// <summary>
        /// Creates a new SqlServerInstanceStore using the supplied settings
        /// </summary>
        /// <param name="settings">Configuration values for the Instnace Store</param>
        public SqlServerInstanceStore(SqlServerInstanceStoreSettings settings)
        {
            ValidateSettings(settings);

            this.settings = settings;
        }

        /// <inheritdoc />
        public int MaxHistoryEntryLength => int.MaxValue;

        /// <inheritdoc />
        public async Task<object> DeleteEntitiesAsync(IEnumerable<InstanceEntityBase> entities)
        {
            using (var connection = await settings.GetDatabaseConnection())
            using (var command = connection.CreateCommand())
            {
                foreach (var entity in entities)
                {
                    if (entity is OrchestrationStateInstanceEntity state)
                    {
                        command.AddStatement($"DELETE FROM {settings.OrchestrationStateTableName} WHERE InstanceId = @instanceId AND ExecutionId = @executionId",
                            new { instanceId = state.State.OrchestrationInstance.InstanceId, executionId = state.State.OrchestrationInstance.ExecutionId });

                    }
                    else if (entity is OrchestrationWorkItemInstanceEntity workItem)
                    {
                        command.AddStatement($"DELETE FROM {settings.WorkItemTableName} WHERE InstanceId = @instanceId AND ExecutionId = @executionId AND SequenceNumber = @sequenceNumber",
                            new { instanceId = workItem.InstanceId, executionId = workItem.ExecutionId, sequenceNumber = workItem.SequenceNumber });
                    }
                    else
                        throw new InvalidOperationException($"Invalid history event type: {entity.GetType()}");
                }

                if (command.CommandText.Any() == false) return null;

                await connection.OpenAsync();
                await command.ExecuteNonQueryAsync();
            }

            return null;
        }

        /// <summary>
        /// Not Supported
        /// </summary>
        public Task<object> DeleteJumpStartEntitiesAsync(IEnumerable<OrchestrationJumpStartInstanceEntity> entities) => throw new NotSupportedException("JumpStart Entities not supported.");

        /// <inheritdoc />
        public async Task DeleteStoreAsync()
        {
            using (var connection = await settings.GetDatabaseConnection())
            using (var command = connection.CreateCommand())
            {
                command.AddStatement($"DROP TABLE IF EXISTS {settings.WorkItemTableName}");
                command.AddStatement($"DROP TABLE IF EXISTS {settings.OrchestrationStateTableName}");

                await connection.OpenAsync();
                await command.ExecuteNonQueryAsync();
            }
        }

        /// <inheritdoc />
        public async Task<IEnumerable<OrchestrationStateInstanceEntity>> GetEntitiesAsync(string instanceId, string executionId)
        {
            using (var connection = await settings.GetDatabaseConnection())
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $"SELECT StateData FROM {settings.OrchestrationStateTableName} WHERE InstanceId = @instanceId AND ExecutionId = @executionId";

                command.AddParameter("instanceId", instanceId)
                    .AddParameter("executionId", executionId);

                await connection.OpenAsync();
                using (var reader = await command.ExecuteReaderAsync())
                {
                    var entities = new List<OrchestrationStateInstanceEntity>();

                    while (await reader.ReadAsync())
                    {
                        entities.Add(new OrchestrationStateInstanceEntity { State = JsonDataConverter.Default.Deserialize<OrchestrationState>(reader.GetString(0)) });
                    }

                    return entities;
                }
            }
        }

        /// <summary>
        /// Not Supported
        /// </summary>
        public Task<IEnumerable<OrchestrationJumpStartInstanceEntity>> GetJumpStartEntitiesAsync(int top) => throw new NotSupportedException("JumpStart Entities not supported.");

        /// <inheritdoc />
        public async Task<IEnumerable<OrchestrationWorkItemInstanceEntity>> GetOrchestrationHistoryEventsAsync(string instanceId, string executionId)
        {
            using (var connection = await settings.GetDatabaseConnection())
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $"SELECT InstanceId, ExecutionId, EventTimestamp, SequenceNumber, HistoryEvent FROM {settings.WorkItemTableName} WHERE InstanceId = @instanceId AND ExecutionId = @executionId ORDER BY SequenceNumber";
                command.AddParameter("instanceId", instanceId)
                    .AddParameter("executionId", executionId);

                await connection.OpenAsync();

                var reader = await command.ExecuteReaderAsync();
                var entities = new List<OrchestrationWorkItemInstanceEntity>();

                while (await reader.ReadAsync())
                {
                    entities.Add(new OrchestrationWorkItemInstanceEntity
                    {
                        InstanceId = reader.GetFieldValue<string>(0),
                        ExecutionId = reader.GetFieldValue<string>(1),
                        EventTimestamp = reader.GetFieldValue<DateTime>(2),
                        SequenceNumber = reader.GetFieldValue<long>(3),
                        HistoryEvent = JsonDataConverter.Default.Deserialize<HistoryEvent>(reader.GetFieldValue<string>(4))
                    });
                }

                return entities;
            }
        }

        /// <inheritdoc />
        public async Task<IEnumerable<OrchestrationStateInstanceEntity>> GetOrchestrationStateAsync(string instanceId, bool allInstances)
        {
            using (var connection = await settings.GetDatabaseConnection())
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $"SELECT StateData FROM {settings.OrchestrationStateTableName} WHERE InstanceId = @instanceId ";
                command.AddParameter("instanceId", instanceId);

                if (allInstances == false)
                {
                    command.CommandText += "AND OrchestrationStatus != @status ";
                    command.AddParameter("status", OrchestrationStatus.ContinuedAsNew.ToString());
                }

                command.CommandText += "ORDER BY LastUpdatedTime";

                await connection.OpenAsync();
                using (var reader = await command.ExecuteReaderAsync())
                {
                    var entities = new List<OrchestrationStateInstanceEntity>();

                    while (await reader.ReadAsync())
                    {
                        entities.Add(new OrchestrationStateInstanceEntity { State = JsonDataConverter.Default.Deserialize<OrchestrationState>(reader.GetFieldValue<string>(0)) });

                        if (allInstances == false) break;
                    }

                    return entities;
                }
            }
        }

        /// <inheritdoc />
        public async Task<OrchestrationStateInstanceEntity> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            using (var connection = await settings.GetDatabaseConnection())
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $"SELECT TOP 1 StateData FROM {settings.OrchestrationStateTableName} WHERE InstanceId = @instanceId AND ExecutionId = @executionId";
                command.AddParameter("instanceId", instanceId)
                    .AddParameter("executionId", executionId);

                await connection.OpenAsync();
                var value = await command.ExecuteScalarAsync();

                return new OrchestrationStateInstanceEntity { State = JsonDataConverter.Default.Deserialize<OrchestrationState>(value.ToString()) };
            }
        }

        /// <inheritdoc />
        public async Task InitializeStoreAsync(bool recreate)
        {
            using (var connection = await settings.GetDatabaseConnection())
            using (var command = connection.CreateCommand())
            {
                if (recreate) await DeleteStoreAsync();

                command.AddStatement($@"IF(SCHEMA_ID(@schema) IS NULL)
                    BEGIN
                        EXEC sp_executesql N'CREATE SCHEMA [{settings.SchemaName}]'
                    END", new { schema = settings.SchemaName });

                command.AddStatement($@"IF(OBJECT_ID(@table) IS NULL)
                    BEGIN
                        CREATE TABLE {settings.OrchestrationStateTableName} (
	                        [InstanceId] NVARCHAR(50) NOT NULL,
	                        [ExecutionId] NVARCHAR(50) NOT NULL,
	                        [Name] NVARCHAR(MAX) NOT NULL,
	                        [Version] NVARCHAR(MAX) NOT NULL,
	                        [OrchestrationStatus] NVARCHAR(50) NOT NULL,
	                        [CreatedTime]  DATETIME2 NOT NULL,
	                        [CompletedTime] DATETIME2 NOT NULL,
	                        [LastUpdatedTime] DATETIME2 NOT NULL,
	                        [StateData] NVARCHAR(MAX) NOT NULL,
                            CONSTRAINT [PK_{settings.SchemaName}_{settings.HubName}{SqlServerInstanceStoreSettings.OrchestrationTable}_InstanceId_ExecutionId] PRIMARY KEY CLUSTERED ([InstanceId], [ExecutionId]))
                    END", new { table = settings.OrchestrationStateTableName });

                command.AddStatement($@"IF(OBJECT_ID(@table) IS NULL)
                    BEGIN
                        CREATE TABLE {settings.WorkItemTableName} (
	                        [InstanceId] NVARCHAR(50) NOT NULL,
	                        [ExecutionId] NVARCHAR(50) NOT NULL,
	                        [SequenceNumber] BIGINT NOT NULL,
	                        [EventTimestamp] DATETIME2 NOT NULL,
	                        [HistoryEvent] NVARCHAR(MAX) NOT NULL,
                            CONSTRAINT [PK_{settings.SchemaName}_{settings.HubName}{SqlServerInstanceStoreSettings.WorkitemTable}_InstanceId_ExecutionId_SequenceNumber] PRIMARY KEY CLUSTERED ([InstanceId], [ExecutionId], [SequenceNumber]))
                    END", new { table = settings.WorkItemTableName });

                await connection.OpenAsync();
                await command.ExecuteNonQueryAsync();
            }
        }

        /// <inheritdoc />
        public async Task<int> PurgeOrchestrationHistoryEventsAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            var deleteStatement = $@"DELETE h FROM {settings.WorkItemTableName} h JOIN {settings.OrchestrationStateTableName} e ON e.InstanceId = h.InstanceId AND e.ExecutionId = h.ExecutionId ";

            switch (timeRangeFilterType)
            {
                case OrchestrationStateTimeRangeFilterType.OrchestrationCompletedTimeFilter:
                    deleteStatement += "WHERE e.CompletedTime <= @thresholdDateTimeUtc";
                    break;
                case OrchestrationStateTimeRangeFilterType.OrchestrationCreatedTimeFilter:
                    deleteStatement += "WHERE e.CreatedTime <= @thresholdDateTimeUtc";
                    break;
                case OrchestrationStateTimeRangeFilterType.OrchestrationLastUpdatedTimeFilter:
                    deleteStatement += "WHERE e.LastUpdatedTime <= @thresholdDateTimeUtc";
                    break;
                default:
                    throw new ArgumentOutOfRangeException($"Unknown {nameof(timeRangeFilterType)} value: {timeRangeFilterType}");
            }

            using (var connection = await settings.GetDatabaseConnection())
            using (var command = connection.CreateCommand())
            {
                command.AddParameter("thresholdDateTimeUtc", thresholdDateTimeUtc)
                    .CommandText = deleteStatement;

                await connection.OpenAsync();
                return await command.ExecuteNonQueryAsync();
            }
        }

        /// <inheritdoc />
        public async Task<object> WriteEntitiesAsync(IEnumerable<InstanceEntityBase> entities)
        {
            using (var connection = await settings.GetDatabaseConnection())
            using (var command = connection.CreateCommand())
            {
                foreach (var entity in entities)
                {
                    if (entity is OrchestrationStateInstanceEntity orchestration)
                    {
                        OrchestrationState state = orchestration.State;
                        command.AddStatement(string.Format(MergeOrchestrationStateInstanceEntityQuery, settings.OrchestrationStateTableName),
                            new
                            {
                                instanceId = state.OrchestrationInstance.InstanceId,
                                executionId = state.OrchestrationInstance.ExecutionId,
                                name = state.Name,
                                version = state.Version,
                                orchestrationStatus = state.OrchestrationStatus.ToString(),
                                createdTime = state.CreatedTime,
                                completedTime = state.CompletedTime,
                                lastUpdatedTime = state.LastUpdatedTime,
                                stateData = JsonDataConverter.Default.Serialize(state)
                            });

                    }
                    else if (entity is OrchestrationWorkItemInstanceEntity workItem)
                    {
                        command.AddStatement(string.Format(MergeOrchestrationWorkItemInstanceEntityQuery, settings.WorkItemTableName),
                            new
                            {
                                instanceId = workItem.InstanceId,
                                executionId = workItem.ExecutionId,
                                sequenceNumber = workItem.SequenceNumber,
                                eventTimestamp = workItem.EventTimestamp,
                                historyEvent = JsonDataConverter.Default.Serialize(workItem.HistoryEvent)
                            });
                    }
                    else
                        throw new InvalidOperationException($"Invalid history event type: {entity.GetType()}");
                }

                if (command.CommandText.Any() == false) return null;

                await connection.OpenAsync();
                await command.ExecuteNonQueryAsync();
            }

            return null;
        }

        /// <summary>
        /// Not Supported
        /// </summary>
        public Task<object> WriteJumpStartEntitiesAsync(IEnumerable<OrchestrationJumpStartInstanceEntity> entities) => throw new NotSupportedException("JumpStart Entities not supported.");

        private void ValidateSettings(SqlServerInstanceStoreSettings settings)
        {
            if (settings.GetDatabaseConnection == null) throw new ArgumentException($"{nameof(settings.GetDatabaseConnection)} cannot be null.");
            if (settings.HubName == null) throw new ArgumentException($"{nameof(settings.HubName)} cannot be null.");
            if (settings.SchemaName == null) throw new ArgumentException($"{nameof(settings.SchemaName)} cannot be null.");

            //Validate schema and hubnames are valid SQL Identifiers
            var sqlIdentifierRegex = new Regex(@"^[\p{L}_][\p{L}\p{N}@$#_]{0,127}$");
            if (sqlIdentifierRegex.IsMatch(settings.SchemaName) == false) throw new ArgumentException($"{nameof(settings.SchemaName)} must be a valid SQL Identifier");
            if (sqlIdentifierRegex.IsMatch(settings.HubName) == false) throw new ArgumentException($"{nameof(settings.HubName)} must be a valid SQL Identifier");
        }


        private const string MergeOrchestrationStateInstanceEntityQuery =
            @"MERGE {0} [Target] USING (VALUES (@instanceId,@executionId,@name,@version,@orchestrationStatus,@createdTime,@completedTime,@lastUpdatedTime,@stateData)) as [Source](InstanceId,ExecutionId,[Name],[Version],OrchestrationStatus,CreatedTime,CompletedTime,LastUpdatedTime,StateData)
                ON [Target].InstanceId = [Source].InstanceId AND [Target].ExecutionId = [Source].ExecutionId
              WHEN NOT MATCHED THEN INSERT (InstanceId,ExecutionId,[Name],[Version],OrchestrationStatus,CreatedTime,CompletedTime,LastUpdatedTime,StateData) VALUES (InstanceId,ExecutionId,[Name],[Version],OrchestrationStatus,CreatedTime,CompletedTime,LastUpdatedTime,StateData)
              WHEN MATCHED THEN UPDATE SET InstanceId = [Source].InstanceId,ExecutionId = [Source].ExecutionId,[Name] = [Source].[Name],[Version] = [Source].[Version],OrchestrationStatus = [Source].OrchestrationStatus,CreatedTime = [Source].CreatedTime,CompletedTime = [Source].CompletedTime,LastUpdatedTime = [Source].LastUpdatedTime,StateData = [Source].StateData;";

        private const string MergeOrchestrationWorkItemInstanceEntityQuery =
            @"MERGE {0} [Target] USING (VALUES (@instanceId,@executionId,@sequenceNumber,@eventTimestamp,@historyEvent)) as [Source](InstanceId,ExecutionId,SequenceNumber,EventTimestamp,HistoryEvent)
                ON [Target].InstanceId = [Source].InstanceId AND [Target].ExecutionId = [Source].ExecutionId AND [Target].SequenceNumber = [Source].SequenceNumber
              WHEN NOT MATCHED THEN INSERT (InstanceId, ExecutionId, SequenceNumber, EventTimestamp, HistoryEvent) VALUES (InstanceId, ExecutionId, SequenceNumber, EventTimestamp, HistoryEvent)
              WHEN MATCHED THEN UPDATE SET EventTimestamp = [Source].EventTimestamp, HistoryEvent = [Source].HistoryEvent;";
    }
}
