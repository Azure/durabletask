﻿//  ----------------------------------------------------------------------------------
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

namespace DurableTask.AzureStorage.Tracking
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Net;
    using System.Reflection;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Tracking store for use with <see cref="AzureStorageOrchestrationService"/>. Uses Azure Tables and Azure Blobs to store runtime state.
    /// </summary>
    class AzureTableTrackingStore : TrackingStoreBase
    {
        const string NameProperty = "Name";
        const string InputProperty = "Input";
        const string ResultProperty = "Result";
        const string OutputProperty = "Output";
        const string RowKeyProperty = "RowKey";
        const string PartitionKeyProperty = "PartitionKey";
        const string SentinelRowKey = "sentinel";
        const string IsCheckpointCompleteProperty = "IsCheckpointComplete";
        const string CheckpointCompletedTimestampProperty = "CheckpointCompletedTimestamp";

        // See https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-the-table-service-data-model#property-types
        const int MaxTablePropertySizeInBytes = 60 * 1024; // 60KB to give buffer

        static readonly string[] VariableSizeEntityProperties = new[]
        {
            NameProperty,
            InputProperty,
            ResultProperty,
            OutputProperty,
            "Reason",
            "Details",
            "Correlation"
        };

        readonly string storageAccountName;
        readonly string taskHubName;
        readonly AzureStorageClient azureStorageClient;
        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly AzureStorageOrchestrationServiceStats stats;
        readonly TableEntityConverter tableEntityConverter;
        readonly IReadOnlyDictionary<EventType, Type> eventTypeMap;
        readonly MessageManager messageManager;

        public AzureTableTrackingStore(
            AzureStorageClient azureStorageClient,
            MessageManager messageManager)
        {
            this.azureStorageClient = azureStorageClient;
            this.messageManager = messageManager;
            this.settings = this.azureStorageClient.Settings;
            this.stats = this.azureStorageClient.Stats;
            this.tableEntityConverter = new TableEntityConverter();
            this.taskHubName = settings.TaskHubName;

            this.storageAccountName = this.azureStorageClient.StorageAccountName;

            string historyTableName = settings.HistoryTableName;
            string instancesTableName = settings.InstanceTableName;

            this.HistoryTable = this.azureStorageClient.GetTableReference(historyTableName);
            this.InstancesTable = this.azureStorageClient.GetTableReference(instancesTableName);

            // Use reflection to learn all the different event types supported by DTFx.
            // This could have been hardcoded, but I generally try to avoid hardcoding of point-in-time DTFx knowledge.
            Type historyEventType = typeof(HistoryEvent);

            IEnumerable<Type> historyEventTypes = historyEventType.Assembly.GetTypes().Where(
                t => !t.IsAbstract && t.IsSubclassOf(historyEventType));

            PropertyInfo eventTypeProperty = historyEventType.GetProperty(nameof(HistoryEvent.EventType));
            this.eventTypeMap = historyEventTypes.ToDictionary(
                type => ((HistoryEvent)FormatterServices.GetUninitializedObject(type)).EventType);
        }

        // For testing
        internal AzureTableTrackingStore(
            AzureStorageOrchestrationServiceStats stats,
            CloudTable instancesTable
        )
        {
            this.stats = stats;
            this.InstancesTable = new Table(stats, instancesTable);
            this.settings = new AzureStorageOrchestrationServiceSettings();
            // Have to set FetchLargeMessageDataEnabled to false, as no MessageManager is 
            // instantiated for this test.
            this.settings.FetchLargeMessageDataEnabled = false;
        }

        internal Table HistoryTable { get; }

        internal Table InstancesTable { get; }

        /// <inheritdoc />
        public override Task CreateAsync()
        {
            return Task.WhenAll(new Task[]
            {
                this.HistoryTable.CreateIfNotExistsAsync(),
                this.InstancesTable.CreateIfNotExistsAsync()
            });
        }

        /// <inheritdoc />
        public override Task DeleteAsync()
        {
            return Task.WhenAll(new Task[]
            {
                this.HistoryTable.DeleteIfExistsAsync(),
                this.InstancesTable.DeleteIfExistsAsync()
            });
        }

        /// <inheritdoc />
        public override async Task<bool> ExistsAsync()
        {
            return this.HistoryTable != null && this.InstancesTable != null && await this.HistoryTable.ExistsAsync() && await this.InstancesTable.ExistsAsync();
        }

        /// <inheritdoc />
        public override async Task<OrchestrationHistory> GetHistoryEventsAsync(string instanceId, string expectedExecutionId, CancellationToken cancellationToken = default(CancellationToken))
        {
            var historyEntitiesResponseInfo = await this.GetHistoryEntitiesResponseInfoAsync(
                instanceId,
                expectedExecutionId,
                null,
                cancellationToken);

            List<DynamicTableEntity> tableEntities = historyEntitiesResponseInfo.ReturnedEntities;

            IList<HistoryEvent> historyEvents;
            string executionId;
            DynamicTableEntity sentinel = null;
            if (tableEntities.Count > 0)
            {
                // The most recent generation will always be in the first history event.
                executionId = tableEntities[0].Properties["ExecutionId"].StringValue;

                // Convert the table entities into history events.
                var events = new List<HistoryEvent>(tableEntities.Count);

                foreach (DynamicTableEntity entity in tableEntities)
                {
                    if (entity.Properties["ExecutionId"].StringValue != executionId)
                    {
                        // The remaining entities are from a previous generation and can be discarded.
                        break;
                    }

                    // The sentinel row does not contain any history events, so save it for later
                    // and continue
                    if (entity.RowKey == SentinelRowKey)
                    {
                        sentinel = entity;
                        continue;
                    }

                    // Some entity properties may be stored in blob storage.
                    await this.DecompressLargeEntityProperties(entity);

                    events.Add((HistoryEvent)this.tableEntityConverter.ConvertFromTableEntity(entity, GetTypeForTableEntity));
                }

                historyEvents = events;
            }
            else
            {
                historyEvents = EmptyHistoryEventList;
                executionId = expectedExecutionId;
            }

            // Read the checkpoint completion time from the sentinel row, which should always be the last row.
            // A sentinel won't exist only if no instance of this ID has ever existed or the instance history
            // was purged.The IsCheckpointCompleteProperty was newly added _after_ v1.6.4.
            DateTime checkpointCompletionTime = DateTime.MinValue;
            sentinel = sentinel ?? tableEntities.LastOrDefault(e => e.RowKey == SentinelRowKey);
            string eTagValue = sentinel?.ETag;
            if (sentinel != null &&
                sentinel.Properties.TryGetValue(CheckpointCompletedTimestampProperty, out EntityProperty timestampProperty))
            {
                checkpointCompletionTime = timestampProperty.DateTime ?? DateTime.MinValue;
            }

            int currentEpisodeNumber = Utils.GetEpisodeNumber(historyEvents);

            this.settings.Logger.FetchedInstanceHistory(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                executionId,
                historyEvents.Count,
                currentEpisodeNumber,
                historyEntitiesResponseInfo.RequestCount,
                historyEntitiesResponseInfo.ElapsedMilliseconds,
                eTagValue,
                checkpointCompletionTime);

            return new OrchestrationHistory(historyEvents, checkpointCompletionTime, eTagValue);
        }

        async Task<TableEntitiesResponseInfo<DynamicTableEntity>> GetHistoryEntitiesResponseInfoAsync(string instanceId, string expectedExecutionId, IList<string> projectionColumns,  CancellationToken cancellationToken = default(CancellationToken))
        {
            var filterCondition = new StringBuilder(200);

            const char Quote = '\'';

            // e.g. "PartitionKey eq 'c138dd969a1e4a699b0644c7d8279f81'"
            var sanitizedInstanceId = KeySanitation.EscapePartitionKey(instanceId);
            filterCondition.Append(PartitionKeyProperty).Append(" eq ").Append(Quote).Append(sanitizedInstanceId).Append(Quote);
            if (!string.IsNullOrEmpty(expectedExecutionId))
            {
                // Filter down to a specific generation.
                // e.g. "PartitionKey eq 'c138dd969a1e4a699b0644c7d8279f81' and (RowKey eq 'sentinel' or ExecutionId eq '85f05ce1494c4a29989f64d3fe0f9089')"
                filterCondition.Append(" and (RowKey eq ").Append(Quote).Append(SentinelRowKey).Append(Quote)
                    .Append(" or ExecutionId eq ").Append(Quote).Append(expectedExecutionId).Append(Quote).Append(')');
            }

            TableQuery query = new TableQuery().Where(filterCondition.ToString());

            if (projectionColumns != null)
            {
                query.Select(projectionColumns);
            }

            var tableEntitiesResponseInfo = await this.HistoryTable.ExecuteQueryAsync(query, cancellationToken);

            return tableEntitiesResponseInfo;
        }

        async Task<List<DynamicTableEntity>> QueryHistoryAsync(string filterCondition, string instanceId, CancellationToken cancellationToken)
        {
            TableQuery query = new TableQuery().Where(filterCondition.ToString());

            var tableEntitiesResponseInfo = await this.HistoryTable.ExecuteQueryAsync(query, cancellationToken);

            var entities = tableEntitiesResponseInfo.ReturnedEntities;

            string executionId = entities.Count > 0 && entities.First().Properties.ContainsKey("ExecutionId") ?
                entities[0].Properties["ExecutionId"].StringValue :
                string.Empty;

            this.settings.Logger.FetchedInstanceHistory(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                executionId,
                entities.Count,
                episode: -1, // We don't have enough information to get the episode number. It's also not important to have for this particular trace.
                tableEntitiesResponseInfo.RequestCount,
                tableEntitiesResponseInfo.ElapsedMilliseconds,
                eTag: string.Empty,
                DateTime.MinValue);

            return entities; 
        }

        public override async Task<IList<string>> RewindHistoryAsync(string instanceId, IList<string> failedLeaves, CancellationToken cancellationToken)
        {
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // REWIND ALGORITHM:
            // 1. Finds failed execution of specified orchestration instance to rewind
            // 2. Finds failure entities to clear and over-writes them (as well as corresponding trigger events)
            // 3. Identifies sub-orchestration failure(s) from parent instance and calls RewindHistoryAsync recursively on failed sub-orchestration child instance(s)
            // 4. Resets orchestration status of rewound instance in instance store table to prepare it to be restarted
            // 5. Returns "failedLeaves", a list of the deepest failed instances on each failed branch to revive with RewindEvent messages
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

            bool hasFailedSubOrchestrations = false;
            const char Quote = '\'';

            var orchestratorStartedFilterCondition = new StringBuilder(200);

            string sanitizedInstanceId = KeySanitation.EscapePartitionKey(instanceId);
            orchestratorStartedFilterCondition.Append(PartitionKeyProperty).Append(" eq ").Append(Quote).Append(sanitizedInstanceId).Append(Quote); // = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, instanceId);
            orchestratorStartedFilterCondition.Append(" and EventType eq ").Append(Quote).Append("OrchestratorStarted").Append(Quote);

            var orchestratorStartedEntities = await this.QueryHistoryAsync(orchestratorStartedFilterCondition.ToString(), instanceId, cancellationToken);

            // get most recent orchestratorStarted event
            var recentStartRowKey = orchestratorStartedEntities.Max(x => x.RowKey);
            var recentStartRow = orchestratorStartedEntities.Where(y => y.RowKey == recentStartRowKey).ToList();
            var executionId = recentStartRow[0].Properties["ExecutionId"].StringValue;
            var instanceTimestamp = recentStartRow[0].Timestamp.DateTime;

            var rowsToUpdateFilterCondition = new StringBuilder(200);

            rowsToUpdateFilterCondition.Append(PartitionKeyProperty).Append(" eq ").Append(Quote).Append(sanitizedInstanceId).Append(Quote);
            rowsToUpdateFilterCondition.Append(" and ExecutionId eq ").Append(Quote).Append(executionId).Append(Quote);
            rowsToUpdateFilterCondition.Append(" and (OrchestrationStatus eq ").Append(Quote).Append("Failed").Append(Quote);
            rowsToUpdateFilterCondition.Append(" or EventType eq ").Append(Quote).Append("TaskFailed").Append(Quote);
            rowsToUpdateFilterCondition.Append(" or EventType eq ").Append(Quote).Append("SubOrchestrationInstanceFailed").Append(Quote).Append(")");

            var entitiesToClear = await this.QueryHistoryAsync(rowsToUpdateFilterCondition.ToString(), instanceId, cancellationToken);

            foreach (DynamicTableEntity entity in entitiesToClear)
            {
                if (entity.Properties["ExecutionId"].StringValue != executionId)
                {
                    // the remaining entities are from a previous generation and can be discarded.
                    break;
                }

                if (entity.RowKey == SentinelRowKey)
                {
                    continue;
                }

                // delete TaskScheduled corresponding to TaskFailed event 
                if (entity.Properties["EventType"].StringValue == nameof(EventType.TaskFailed))
                {
                    var taskScheduledId = entity.Properties["TaskScheduledId"].Int32Value.ToString();

                    var tsFilterCondition = new StringBuilder(200);

                    tsFilterCondition.Append(PartitionKeyProperty).Append(" eq ").Append(Quote).Append(sanitizedInstanceId).Append(Quote);
                    tsFilterCondition.Append(" and ExecutionId eq ").Append(Quote).Append(executionId).Append(Quote);
                    tsFilterCondition.Append(" and EventId eq ").Append(taskScheduledId);
                    tsFilterCondition.Append(" and EventType eq ").Append(Quote).Append(nameof(EventType.TaskScheduled)).Append(Quote);

                    var taskScheduledEntities = await QueryHistoryAsync(tsFilterCondition.ToString(), instanceId, cancellationToken);

                    taskScheduledEntities[0].Properties["Reason"] = new EntityProperty("Rewound: " + taskScheduledEntities[0].Properties["EventType"].StringValue);
                    taskScheduledEntities[0].Properties["EventType"] = new EntityProperty(nameof(EventType.GenericEvent));
                    
                    await this.HistoryTable.ReplaceAsync(taskScheduledEntities[0]);
                }

                // delete SubOrchestratorCreated corresponding to SubOrchestraionInstanceFailed event
                if (entity.Properties["EventType"].StringValue == nameof(EventType.SubOrchestrationInstanceFailed))
                {
                    hasFailedSubOrchestrations = true;
                    var subOrchestrationId = entity.Properties["TaskScheduledId"].Int32Value.ToString();

                    var soFilterCondition = new StringBuilder(200);

                    soFilterCondition.Append(PartitionKeyProperty).Append(" eq ").Append(Quote).Append(sanitizedInstanceId).Append(Quote);
                    soFilterCondition.Append(" and ExecutionId eq ").Append(Quote).Append(executionId).Append(Quote);
                    soFilterCondition.Append(" and EventId eq ").Append(subOrchestrationId);
                    soFilterCondition.Append(" and EventType eq ").Append(Quote).Append(nameof(EventType.SubOrchestrationInstanceCreated)).Append(Quote);

                    var subOrchesratrationEntities = await QueryHistoryAsync(soFilterCondition.ToString(), instanceId, cancellationToken);

                    var soInstanceId = subOrchesratrationEntities[0].Properties["InstanceId"].StringValue;

                    // the SubORchestrationCreatedEvent is still healthy and will not be overwritten, just marked as rewound
                    subOrchesratrationEntities[0].Properties["Reason"] = new EntityProperty("Rewound: " + subOrchesratrationEntities[0].Properties["EventType"].StringValue);

                    await this.HistoryTable.ReplaceAsync(subOrchesratrationEntities[0]);

                    // recursive call to clear out failure events on child instances
                    await this.RewindHistoryAsync(soInstanceId, failedLeaves, cancellationToken);
                }

                // "clear" failure event by making RewindEvent: replay ignores row while dummy event preserves rowKey
                entity.Properties["Reason"] = new EntityProperty("Rewound: " + entity.Properties["EventType"].StringValue);
                entity.Properties["EventType"] = new EntityProperty(nameof(EventType.GenericEvent));

                await this.HistoryTable.ReplaceAsync(entity);
            }

            // reset orchestration status in instance store table
            await UpdateStatusForRewindAsync(instanceId);

            if (!hasFailedSubOrchestrations)
            {
                failedLeaves.Add(instanceId);
            }

            return failedLeaves;
        }

        /// <inheritdoc />
        public override async Task<IList<OrchestrationState>> GetStateAsync(string instanceId, bool allExecutions, bool fetchInput)
        {
            return new[] { await this.GetStateAsync(instanceId, executionId: null, fetchInput: fetchInput) };
        }

        /// <inheritdoc />
        public override async Task<OrchestrationState> GetStateAsync(string instanceId, string executionId, bool fetchInput)
        {
            InstanceStatus instanceStatus = await this.FetchInstanceStatusInternalAsync(instanceId, executionId, fetchInput);
            return instanceStatus?.State;
        }

        /// <inheritdoc />
        public override Task<InstanceStatus> FetchInstanceStatusAsync(string instanceId)
        {
            return this.FetchInstanceStatusInternalAsync(instanceId, executionId: null, fetchInput: false);
        }

        /// <inheritdoc />
        async Task<InstanceStatus> FetchInstanceStatusInternalAsync(string instanceId, string executionId, bool fetchInput)
        {
            if (instanceId == null)
            {
                throw new ArgumentNullException(nameof(instanceId));
            }

            var queryCondition = new OrchestrationInstanceStatusQueryCondition
            {
                InstanceId = instanceId,
                FetchInput = fetchInput,
            };

            var tableEntitiesResponseInfo = await this.InstancesTable.ExecuteQueryAsync(queryCondition.ToTableQuery<DynamicTableEntity>());

            this.settings.Logger.FetchedInstanceStatus(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                executionId ?? string.Empty,
                tableEntitiesResponseInfo.ElapsedMilliseconds);

            var tableEntity = tableEntitiesResponseInfo.ReturnedEntities.ToList().FirstOrDefault();
            if (tableEntity == null)
            {
                return null;
            }

            var orchestrationState = await this.ConvertFromAsync(tableEntity);

            return new InstanceStatus(orchestrationState, tableEntity.ETag);
        }

        Task<OrchestrationState> ConvertFromAsync(DynamicTableEntity tableEntity)
        {
            var properties = tableEntity.Properties;
            var orchestrationInstanceStatus = ConvertFromAsync(properties);
            var instanceId = KeySanitation.UnescapePartitionKey(tableEntity.PartitionKey);
            return ConvertFromAsync(orchestrationInstanceStatus, instanceId);
        }

        static OrchestrationInstanceStatus ConvertFromAsync(IDictionary<string, EntityProperty> properties)
        {
            var orchestrationInstanceStatus = new OrchestrationInstanceStatus();

            var type = typeof(OrchestrationInstanceStatus);
            foreach (var pair in properties)
            {
                var property = type.GetProperty(pair.Key);
                if (property != null)
                {
                    var value = pair.Value;
                    if (value != null)
                    {
                        if (property.PropertyType == typeof(DateTime) || property.PropertyType == typeof(DateTime?))
                        {
                            property.SetValue(orchestrationInstanceStatus, value.DateTime);
                        }
                        else
                        {
                            property.SetValue(orchestrationInstanceStatus, value.StringValue);
                        }
                    }
                }
            }

            return orchestrationInstanceStatus;
        }

        async Task<OrchestrationState> ConvertFromAsync(OrchestrationInstanceStatus orchestrationInstanceStatus, string instanceId)
        {
            var orchestrationState = new OrchestrationState();
            if (!Enum.TryParse(orchestrationInstanceStatus.RuntimeStatus, out orchestrationState.OrchestrationStatus))
            {
                // This is not expected, but could happen if there is invalid data in the Instances table.
                orchestrationState.OrchestrationStatus = (OrchestrationStatus)(-1);
            }

            orchestrationState.OrchestrationInstance = new OrchestrationInstance
            {
                InstanceId = instanceId,
                ExecutionId = orchestrationInstanceStatus.ExecutionId,
            };

            orchestrationState.Name = orchestrationInstanceStatus.Name;
            orchestrationState.Version = orchestrationInstanceStatus.Version;
            orchestrationState.Status = orchestrationInstanceStatus.CustomStatus;
            orchestrationState.CreatedTime = orchestrationInstanceStatus.CreatedTime;
            orchestrationState.CompletedTime = orchestrationInstanceStatus.CompletedTime.GetValueOrDefault();
            orchestrationState.LastUpdatedTime = orchestrationInstanceStatus.LastUpdatedTime;
            orchestrationState.Input = orchestrationInstanceStatus.Input;
            orchestrationState.Output = orchestrationInstanceStatus.Output;
            orchestrationState.ScheduledStartTime = orchestrationInstanceStatus.ScheduledStartTime;

            if (this.settings.FetchLargeMessageDataEnabled)
            {
                orchestrationState.Input = await this.messageManager.FetchLargeMessageIfNecessary(orchestrationState.Input);
                orchestrationState.Output = await this.messageManager.FetchLargeMessageIfNecessary(orchestrationState.Output);
            }

            return orchestrationState;
        }

        /// <inheritdoc />
        public override async Task<IList<OrchestrationState>> GetStateAsync(IEnumerable<string> instanceIds)
        {
            if (instanceIds == null || !instanceIds.Any())
            {
                return Array.Empty<OrchestrationState>();
            }

            // In theory this could exceed MaxStorageOperationConcurrency, but the hard maximum of parallel requests is tied to control queue
            // batch size, which is generally roughly the same value as MaxStorageOperationConcurrency. In almost every case, we would expect this
            // to only be a small handful of parallel requests, so keeping the code simple until the storage refactor adds global throttling.
            var instanceQueries = instanceIds.Select(instance => this.GetStateAsync(instance, allExecutions: true, fetchInput: false));
            IEnumerable<IList<OrchestrationState>> instanceQueriesResults = await Task.WhenAll(instanceQueries);
            return instanceQueriesResults.SelectMany(result => result).Where(orchestrationState => orchestrationState != null).ToList();
        }

        /// <inheritdoc />
        public override Task<IList<OrchestrationState>> GetStateAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            TableQuery<OrchestrationInstanceStatus> query = new TableQuery<OrchestrationInstanceStatus>().
                Where(TableQuery.GenerateFilterCondition(RowKeyProperty, QueryComparisons.Equal, string.Empty));
            return this.QueryStateAsync(query, cancellationToken);
        }

        public override Task<IList<OrchestrationState>> GetStateAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus, CancellationToken cancellationToken = default(CancellationToken))
        {
            return this.QueryStateAsync(OrchestrationInstanceStatusQueryCondition.Parse(createdTimeFrom, createdTimeTo, runtimeStatus)
                            .ToTableQuery<OrchestrationInstanceStatus>(), cancellationToken);
        }

        public override Task<DurableStatusQueryResult> GetStateAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus, int top, string continuationToken, CancellationToken cancellationToken = default(CancellationToken))
        {
            return this.QueryStateAsync(
                OrchestrationInstanceStatusQueryCondition.Parse(createdTimeFrom, createdTimeTo, runtimeStatus)
                    .ToTableQuery<OrchestrationInstanceStatus>(),
                top,
                continuationToken,
                cancellationToken);
        }

        public override Task<DurableStatusQueryResult> GetStateAsync(OrchestrationInstanceStatusQueryCondition condition, int top, string continuationToken, CancellationToken cancellationToken = default(CancellationToken))
        {
            return this.QueryStateAsync(
                condition.ToTableQuery<OrchestrationInstanceStatus>(),
                top,
                continuationToken,
                cancellationToken);
        }

        async Task<DurableStatusQueryResult> QueryStateAsync(TableQuery<OrchestrationInstanceStatus> query, int top, string continuationToken, CancellationToken cancellationToken)
        {
            var orchestrationStates = new List<OrchestrationState>(top);
            query.Take(top);

            var tableEntitiesResponseInfo = await this.InstancesTable.ExecuteQueryAsync(query, cancellationToken, continuationToken);

            IEnumerable<OrchestrationState> result = await Task.WhenAll(tableEntitiesResponseInfo.ReturnedEntities.Select( status => this.ConvertFromAsync(status, KeySanitation.UnescapePartitionKey(status.PartitionKey))));
            orchestrationStates.AddRange(result);

            var queryResult = new DurableStatusQueryResult()
            {
                OrchestrationState = orchestrationStates,
            };

            return queryResult;
        }

        async Task<IList<OrchestrationState>> QueryStateAsync(TableQuery<OrchestrationInstanceStatus> query, CancellationToken cancellationToken)
        {
            var orchestrationStates = new List<OrchestrationState>(100);

            var tableEntitiesResponseInfo = await this.InstancesTable.ExecuteQueryAsync(query, cancellationToken);   

            IEnumerable<OrchestrationState> result = await Task.WhenAll(tableEntitiesResponseInfo.ReturnedEntities.Select(
                status => this.ConvertFromAsync(status, KeySanitation.UnescapePartitionKey(status.PartitionKey))));

            orchestrationStates.AddRange(result);

            return orchestrationStates;
        }

        async Task<PurgeHistoryResult> DeleteHistoryAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus)
        {
            TableQuery<OrchestrationInstanceStatus> query = OrchestrationInstanceStatusQueryCondition.Parse(createdTimeFrom, createdTimeTo, runtimeStatus)
                .ToTableQuery<OrchestrationInstanceStatus>();

            int storageRequests = 0;
            int rowsDeleted = 0;

            var tableEntitiesResponseInfo = await this.InstancesTable.ExecuteQueryAsync(query);
            var results = tableEntitiesResponseInfo.ReturnedEntities;

                foreach (OrchestrationInstanceStatus orchestrationInstanceStatus in results)
                {
                    var statisticsFromDeletion = await this.DeleteAllDataForOrchestrationInstance(orchestrationInstanceStatus);
                    storageRequests += statisticsFromDeletion.StorageRequests;
                    rowsDeleted += statisticsFromDeletion.RowsDeleted;
                }

            return new PurgeHistoryResult(storageRequests, results.Count, rowsDeleted);
        }

        async Task<PurgeHistoryResult> DeleteAllDataForOrchestrationInstance(OrchestrationInstanceStatus orchestrationInstanceStatus)
        {
            int storageRequests = 0;
            int rowsDeleted = 0;
            var historyEntitiesResponseInfo = await this.GetHistoryEntitiesResponseInfoAsync(
                KeySanitation.UnescapePartitionKey(orchestrationInstanceStatus.PartitionKey),
                null,
                new []
                {
                    RowKeyProperty
                });
            storageRequests += historyEntitiesResponseInfo.RequestCount;

            var historyEntities = historyEntitiesResponseInfo.ReturnedEntities;

            await this.messageManager.DeleteLargeMessageBlobs(orchestrationInstanceStatus.PartitionKey);

            var deletedEntitiesResponseInfo = await this.HistoryTable.DeleteBatchAsync(historyEntities);
            storageRequests += deletedEntitiesResponseInfo.RequestCount;

            await this.InstancesTable.DeleteAsync(new DynamicTableEntity
                    {
                        PartitionKey = orchestrationInstanceStatus.PartitionKey,
                        RowKey = string.Empty,
                        ETag = "*"
                    });

            storageRequests++;

            return new PurgeHistoryResult(storageRequests, 1, rowsDeleted);
        }

        /// <inheritdoc />
        public override Task PurgeHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public override async Task<PurgeHistoryResult> PurgeInstanceHistoryAsync(string instanceId)
        {
            string sanitizedInstanceId = KeySanitation.EscapePartitionKey(instanceId);

            TableQuery<OrchestrationInstanceStatus> query = new TableQuery<OrchestrationInstanceStatus>().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition(PartitionKeyProperty, QueryComparisons.Equal, sanitizedInstanceId),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition(RowKeyProperty, QueryComparisons.Equal, string.Empty)));

            var tableEntitiesResponseInfo = await this.InstancesTable.ExecuteQueryAsync(query);

            OrchestrationInstanceStatus orchestrationInstanceStatus = tableEntitiesResponseInfo.ReturnedEntities.FirstOrDefault();

            if (orchestrationInstanceStatus != null)
            {
                PurgeHistoryResult result = await this.DeleteAllDataForOrchestrationInstance(orchestrationInstanceStatus);

                this.settings.Logger.PurgeInstanceHistory(
                    this.storageAccountName,
                    this.taskHubName,
                    instanceId,
                    DateTime.MinValue.ToString(),
                    DateTime.MinValue.ToString(),
                    string.Empty,
                    result.StorageRequests,
                    result.InstancesDeleted,
                    tableEntitiesResponseInfo.ElapsedMilliseconds);

                return result;
            }

            return new PurgeHistoryResult(0, 0, 0);
        }

        /// <inheritdoc />
        public override async Task<PurgeHistoryResult> PurgeInstanceHistoryAsync(
            DateTime createdTimeFrom,
            DateTime? createdTimeTo,
            IEnumerable<OrchestrationStatus> runtimeStatus)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            List<OrchestrationStatus> runtimeStatusList =  runtimeStatus?.Where(
               status => status == OrchestrationStatus.Completed ||
                    status == OrchestrationStatus.Terminated ||
                    status == OrchestrationStatus.Canceled ||
                    status == OrchestrationStatus.Failed).ToList();

            PurgeHistoryResult result = await this.DeleteHistoryAsync(createdTimeFrom, createdTimeTo, runtimeStatusList);

            this.settings.Logger.PurgeInstanceHistory(
                this.storageAccountName,
                this.taskHubName,
                string.Empty,
                createdTimeFrom.ToString(),
                createdTimeTo.ToString() ?? DateTime.MinValue.ToString(),
                runtimeStatus != null ?
                    string.Join(",", runtimeStatus.Select(x => x.ToString())) :
                    string.Empty,
                result.StorageRequests,
                result.InstancesDeleted,
                stopwatch.ElapsedMilliseconds);

            return result;
        }

        /// <inheritdoc />
        public override async Task<bool> SetNewExecutionAsync(
            ExecutionStartedEvent executionStartedEvent,
            string eTag,
            string inputStatusOverride)
        {
            string sanitizedInstanceId = KeySanitation.EscapePartitionKey(executionStartedEvent.OrchestrationInstance.InstanceId);
            DynamicTableEntity entity = new DynamicTableEntity(sanitizedInstanceId, "")
            {
                ETag = eTag,
                Properties =
                {
                    ["Input"] = new EntityProperty(inputStatusOverride ?? executionStartedEvent.Input),
                    ["CreatedTime"] = new EntityProperty(executionStartedEvent.Timestamp),
                    ["Name"] = new EntityProperty(executionStartedEvent.Name),
                    ["Version"] = new EntityProperty(executionStartedEvent.Version),
                    ["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.Pending.ToString()),
                    ["LastUpdatedTime"] = new EntityProperty(DateTime.UtcNow),
                    ["TaskHubName"] = new EntityProperty(this.settings.TaskHubName),
                    ["ScheduledStartTime"] = new EntityProperty(executionStartedEvent.ScheduledStartTime),
                    ["ExecutionId"] = new EntityProperty(executionStartedEvent.OrchestrationInstance.ExecutionId),
                }
            };

            // It is possible that the queue message was small enough to be written directly to a queue message,
            // not a blob, but is too large to be written to a table property.
            await this.CompressLargeMessageAsync(entity);

            Stopwatch stopwatch = Stopwatch.StartNew();
            try
            {
                await this.InstancesTable.InsertOrReplaceAsync(entity);
            }
            catch (DurableTaskStorageException e) when (
                e.HttpStatusCode == 409 /* Conflict */ ||
                e.HttpStatusCode == 412 /* Precondition failed */)
            {
                // Ignore. The main scenario for this is handling race conditions in status update.
                return false;
            }

            // Episode 0 means the orchestrator hasn't started yet.
            int currentEpisodeNumber = 0;

            this.settings.Logger.InstanceStatusUpdate(
                this.storageAccountName,
                this.taskHubName,
                executionStartedEvent.OrchestrationInstance.InstanceId,
                executionStartedEvent.OrchestrationInstance.ExecutionId,
                OrchestrationStatus.Pending,
                currentEpisodeNumber,
                stopwatch.ElapsedMilliseconds);

            return true;
        }

        /// <inheritdoc />
        public override async Task UpdateStatusForRewindAsync(string instanceId)
        {
            string sanitizedInstanceId = KeySanitation.EscapePartitionKey(instanceId);
            DynamicTableEntity entity = new DynamicTableEntity(sanitizedInstanceId, "")
            {
                ETag = "*",
                Properties =
                {
                    ["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.Pending.ToString()),
                    ["LastUpdatedTime"] = new EntityProperty(DateTime.UtcNow),
                }
            };

            Stopwatch stopwatch = Stopwatch.StartNew();
            await this.InstancesTable.MergeAsync(entity);

            // We don't have enough information to get the episode number.
            // It's also not important to have for this particular trace.
            int currentEpisodeNumber = 0;

            this.settings.Logger.InstanceStatusUpdate(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                string.Empty,
                OrchestrationStatus.Pending,
                currentEpisodeNumber,
                stopwatch.ElapsedMilliseconds);
        }


        /// <inheritdoc />
        public override Task StartAsync()
        {
            ServicePointManager.FindServicePoint(this.HistoryTable.Uri).UseNagleAlgorithm = false;
            ServicePointManager.FindServicePoint(this.InstancesTable.Uri).UseNagleAlgorithm = false;
            return Utils.CompletedTask;
        }

        /// <inheritdoc />
        public override async Task<string> UpdateStateAsync(
            OrchestrationRuntimeState newRuntimeState,
            OrchestrationRuntimeState oldRuntimeState,
            string instanceId,
            string executionId,
            string eTagValue)
        {
            int estimatedBytes = 0;
            IList<HistoryEvent> newEvents = newRuntimeState.NewEvents;
            IList<HistoryEvent> allEvents = newRuntimeState.Events;

            int episodeNumber = Utils.GetEpisodeNumber(newRuntimeState);

            var newEventListBuffer = new StringBuilder(4000);
            var historyEventBatch = new TableBatchOperation();

            OrchestrationStatus runtimeStatus = OrchestrationStatus.Running;
            string sanitizedInstanceId = KeySanitation.EscapePartitionKey(instanceId);

            var instanceEntity = new DynamicTableEntity(sanitizedInstanceId, string.Empty)
            {
                Properties =
                {
                    // TODO: Translating null to "null" is a temporary workaround. We should prioritize 
                    // https://github.com/Azure/durabletask/issues/477 so that this is no longer necessary.
                    ["CustomStatus"] = new EntityProperty(newRuntimeState.Status ?? "null"),
                    ["ExecutionId"] = new EntityProperty(executionId),
                    ["LastUpdatedTime"] = new EntityProperty(newEvents.Last().Timestamp),
                }
            };
           
            for (int i = 0; i < newEvents.Count; i++)
            {
                bool isFinalEvent = i == newEvents.Count - 1;

                HistoryEvent historyEvent = newEvents[i];
                var historyEntity = this.tableEntityConverter.ConvertToTableEntity(historyEvent);
                historyEntity.PartitionKey = sanitizedInstanceId;

                newEventListBuffer.Append(historyEvent.EventType.ToString()).Append(',');

                // The row key is the sequence number, which represents the chronological ordinal of the event.
                long sequenceNumber = i + (allEvents.Count - newEvents.Count);
                historyEntity.RowKey = sequenceNumber.ToString("X16");
                historyEntity.Properties["ExecutionId"] = new EntityProperty(executionId);

                await this.CompressLargeMessageAsync(historyEntity);

                // Replacement can happen if the orchestration episode gets replayed due to a commit failure in one of the steps below.
                historyEventBatch.InsertOrReplace(historyEntity);

                // Keep track of the byte count to ensure we don't hit the 4 MB per-batch maximum
                estimatedBytes += GetEstimatedByteCount(historyEntity);

                // Monitor for orchestration instance events 
                switch (historyEvent.EventType)
                {
                    case EventType.ExecutionStarted:
                        runtimeStatus = OrchestrationStatus.Running;
                        ExecutionStartedEvent executionStartedEvent = (ExecutionStartedEvent)historyEvent;
                        instanceEntity.Properties["Name"] = new EntityProperty(executionStartedEvent.Name);
                        instanceEntity.Properties["Version"] = new EntityProperty(executionStartedEvent.Version);
                        instanceEntity.Properties["CreatedTime"] = new EntityProperty(executionStartedEvent.Timestamp);
                        instanceEntity.Properties["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.Running.ToString());
                        if (executionStartedEvent.ScheduledStartTime.HasValue) {
                            instanceEntity.Properties["ScheduledStartTime"] = new EntityProperty(executionStartedEvent.ScheduledStartTime);
                        }

                        CorrelationTraceClient.Propagate(() =>
                        {
                            historyEntity.Properties["Correlation"] = new EntityProperty(executionStartedEvent.Correlation);
                            estimatedBytes += Encoding.Unicode.GetByteCount(executionStartedEvent.Correlation);
                        });

                        this.SetInstancesTablePropertyFromHistoryProperty(
                            historyEntity,
                            instanceEntity,
                            historyPropertyName: nameof(executionStartedEvent.Input),
                            instancePropertyName: InputProperty,
                            data: executionStartedEvent.Input);
                        break;
                    case EventType.ExecutionCompleted:
                        ExecutionCompletedEvent executionCompleted = (ExecutionCompletedEvent)historyEvent;
                        runtimeStatus = executionCompleted.OrchestrationStatus;
                        instanceEntity.Properties["RuntimeStatus"] = new EntityProperty(executionCompleted.OrchestrationStatus.ToString());
                        instanceEntity.Properties["CompletedTime"] = new EntityProperty(DateTime.UtcNow);
                        this.SetInstancesTablePropertyFromHistoryProperty(
                            historyEntity,
                            instanceEntity,
                            historyPropertyName: nameof(executionCompleted.Result),
                            instancePropertyName: OutputProperty,
                            data: executionCompleted.Result);
                        break;
                    case EventType.ExecutionTerminated:
                        runtimeStatus = OrchestrationStatus.Terminated;
                        ExecutionTerminatedEvent executionTerminatedEvent = (ExecutionTerminatedEvent)historyEvent;
                        instanceEntity.Properties["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.Terminated.ToString());
                        instanceEntity.Properties["CompletedTime"] = new EntityProperty(DateTime.UtcNow);
                        this.SetInstancesTablePropertyFromHistoryProperty(
                            historyEntity,
                            instanceEntity,
                            historyPropertyName: nameof(executionTerminatedEvent.Input),
                            instancePropertyName: OutputProperty,
                            data: executionTerminatedEvent.Input);
                        break;
                    case EventType.ContinueAsNew:
                        runtimeStatus = OrchestrationStatus.ContinuedAsNew;
                        ExecutionCompletedEvent executionCompletedEvent = (ExecutionCompletedEvent)historyEvent;
                        instanceEntity.Properties["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.ContinuedAsNew.ToString());
                        this.SetInstancesTablePropertyFromHistoryProperty(
                            historyEntity,
                            instanceEntity,
                            historyPropertyName: nameof(executionCompletedEvent.Result),
                            instancePropertyName: OutputProperty,
                            data: executionCompletedEvent.Result);
                        break;
                }

                // Table storage only supports inserts of up to 100 entities at a time or 4 MB at a time.
                if (historyEventBatch.Count == 99 || estimatedBytes > 3 * 1024 * 1024 /* 3 MB */)
                {
                    eTagValue = await this.UploadHistoryBatch(
                        instanceId,
                        sanitizedInstanceId,
                        executionId,
                        historyEventBatch,
                        newEventListBuffer,
                        allEvents.Count,
                        episodeNumber,
                        estimatedBytes,
                        eTagValue,
                        isFinalBatch: isFinalEvent);

                    // Reset local state for the next batch
                    newEventListBuffer.Clear();
                    historyEventBatch.Clear();
                    estimatedBytes = 0;
                }
            }

            // First persistence step is to commit history to the history table. Messages must come after.
            if (historyEventBatch.Count > 0)
            {
                eTagValue = await this.UploadHistoryBatch(
                    instanceId,
                    sanitizedInstanceId,
                    executionId,
                    historyEventBatch,
                    newEventListBuffer,
                    allEvents.Count,
                    episodeNumber,
                    estimatedBytes,
                    eTagValue,
                    isFinalBatch: true);
            }

            Stopwatch orchestrationInstanceUpdateStopwatch = Stopwatch.StartNew();
            await this.InstancesTable.InsertOrMergeAsync(instanceEntity);

            this.settings.Logger.InstanceStatusUpdate(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                executionId,
                runtimeStatus,
                episodeNumber,
                orchestrationInstanceUpdateStopwatch.ElapsedMilliseconds);

            return eTagValue;
        }

        static int GetEstimatedByteCount(DynamicTableEntity entity)
        {
            // Assume at least 1 KB of data per entity to account for static-length properties
            int estimatedByteCount = 1024;

            // Count the bytes for variable-length properties, which are assumed to always be strings
            foreach (string propertyName in VariableSizeEntityProperties)
            {
                EntityProperty property;
                if (entity.Properties.TryGetValue(propertyName, out property) && !string.IsNullOrEmpty(property.StringValue))
                {
                    estimatedByteCount += Encoding.Unicode.GetByteCount(property.StringValue);
                }
            }

            return estimatedByteCount;
        }

        Type GetTypeForTableEntity(DynamicTableEntity tableEntity)
        {
            string propertyName = nameof(HistoryEvent.EventType);

            EntityProperty eventTypeProperty;
            if (!tableEntity.Properties.TryGetValue(propertyName, out eventTypeProperty))
            {
                throw new ArgumentException($"The DynamicTableEntity did not contain a '{propertyName}' property.");
            }

            if (eventTypeProperty.PropertyType != EdmType.String)
            {
                throw new ArgumentException($"The DynamicTableEntity's {propertyName} property type must a String.");
            }

            EventType eventType;
            if (!Enum.TryParse(eventTypeProperty.StringValue, out eventType))
            {
                throw new ArgumentException($"{eventTypeProperty.StringValue} is not a valid EventType value.");
            }

            return this.eventTypeMap[eventType];
        }

        // Assigns the target table entity property. Any large message for type 'Input, or 'Output' would have been compressed earlier as part of the 'entity' object,
        // so, we only need to assign the 'entity' object's blobName to the target table entity blob name property.
        void SetInstancesTablePropertyFromHistoryProperty(
            DynamicTableEntity historyEntity,
            DynamicTableEntity instanceEntity,
            string historyPropertyName,
            string instancePropertyName,
            string data)
        {
            string blobPropertyName = GetBlobPropertyName(historyPropertyName);
            if (historyEntity.Properties.TryGetValue(blobPropertyName, out EntityProperty blobProperty))
            {
                // This is a large message
                string blobName = blobProperty.StringValue;
                string blobUrl = this.messageManager.GetBlobUrl(blobName);
                instanceEntity.Properties[instancePropertyName] = new EntityProperty(blobUrl);
            }
            else
            {
                // This is a normal-sized message and can be stored inline
                instanceEntity.Properties[instancePropertyName] = new EntityProperty(data);
            }
        }

        async Task CompressLargeMessageAsync(DynamicTableEntity entity)
        {
            foreach (string propertyName in VariableSizeEntityProperties)
            {
                if (entity.Properties.TryGetValue(propertyName, out EntityProperty property) &&
                    this.ExceedsMaxTablePropertySize(property.StringValue))
                {
                    // Upload the large property as a blob in Blob Storage since it won't fit in table storage.
                    string blobName = GetBlobName(entity, propertyName);
                    byte[] messageBytes = Encoding.UTF8.GetBytes(entity.Properties[propertyName].StringValue);
                    await this.messageManager.CompressAndUploadAsBytesAsync(messageBytes, blobName);

                    // Clear out the original property value and create a new "*BlobName"-suffixed property.
                    // The runtime will look for the new "*BlobName"-suffixed column to know if a property is stored in a blob.
                    string blobPropertyName = GetBlobPropertyName(propertyName);
                    entity.Properties.Add(blobPropertyName, new EntityProperty(blobName));
                    entity.Properties[propertyName].StringValue = string.Empty;
                }
            }
        }

        async Task DecompressLargeEntityProperties(DynamicTableEntity entity)
        {
            // Check for entity properties stored in blob storage
            foreach (string propertyName in VariableSizeEntityProperties)
            {
                string blobPropertyName = GetBlobPropertyName(propertyName);
                if (entity.Properties.TryGetValue(blobPropertyName, out EntityProperty property))
                {
                    string blobName = property.StringValue;
                    string decompressedMessage = await this.messageManager.DownloadAndDecompressAsBytesAsync(blobName);
                    entity.Properties[propertyName] = new EntityProperty(decompressedMessage);
                    entity.Properties.Remove(blobPropertyName);
                }
            }
        }

        static string GetBlobPropertyName(string originalPropertyName)
        {
            // WARNING: Changing this is a breaking change!
            return originalPropertyName + "BlobName";
        }

        static string GetBlobName(DynamicTableEntity entity, string property)
        {
            string sanitizedInstanceId = entity.PartitionKey;
            string sequenceNumber = entity.RowKey;

            string eventType;
            if (entity.Properties.ContainsKey("EventType"))
            {
                eventType = entity.Properties["EventType"].StringValue;
            }
            else if (property == "Input")
            {
                // This message is just to start the orchestration, so it does not have a corresponding
                // EventType. Use a hardcoded value to record the orchestration input.
                eventType = "Input";
            }
            else
            {
                throw new InvalidOperationException($"Could not compute the blob name for property {property}");
            }

            string blobName = $"{sanitizedInstanceId}/history-{sequenceNumber}-{eventType}-{property}.json.gz";

            return blobName;
        }

        async Task<string> UploadHistoryBatch(
            string instanceId,
            string sanitizedInstanceId,
            string executionId,
            TableBatchOperation historyEventBatch,
            StringBuilder historyEventNamesBuffer,
            int numberOfTotalEvents,
            int episodeNumber,
            int estimatedBatchSizeInBytes,
            string eTagValue,
            bool isFinalBatch)
        {
            // Adding / updating sentinel entity
            DynamicTableEntity sentinelEntity = new DynamicTableEntity(sanitizedInstanceId, SentinelRowKey)
            {
                Properties =
                {
                    ["ExecutionId"] = new EntityProperty(executionId),
                    [IsCheckpointCompleteProperty] = new EntityProperty(isFinalBatch),
                }
            };

            if (isFinalBatch)
            {
                sentinelEntity.Properties[CheckpointCompletedTimestampProperty] = new EntityProperty(DateTime.UtcNow);
            }

            if (!string.IsNullOrEmpty(eTagValue))
            {
                sentinelEntity.ETag = eTagValue;
                historyEventBatch.Merge(sentinelEntity);
            }
            else
            {
                historyEventBatch.Insert(sentinelEntity);
            }

            TableResultResponseInfo resultInfo;
            Stopwatch stopwatch = Stopwatch.StartNew();
            try
            {
                resultInfo = await this.HistoryTable.ExecuteBatchAsync(historyEventBatch, "InsertOrReplace History");
                this.stats.TableEntitiesWritten.Increment(historyEventBatch.Count);
            }
            catch (DurableTaskStorageException ex)
            {
                if (ex.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                {
                    this.settings.Logger.SplitBrainDetected(
                        this.storageAccountName,
                        this.taskHubName,
                        instanceId,
                        executionId,
                        historyEventBatch.Count - 1, // exclude sentinel from count
                        numberOfTotalEvents,
                        historyEventNamesBuffer.ToString(0, historyEventNamesBuffer.Length - 1), // remove trailing comma
                        stopwatch.ElapsedMilliseconds,
                        eTagValue);
                }

                throw;
            }

            var tableResultList = resultInfo.TableResults;
            string newETagValue = null;
            for (int i = tableResultList.Count - 1; i >= 0; i--)
            {
                DynamicTableEntity resultEntity = (DynamicTableEntity)tableResultList[i].Result;
                if (resultEntity.RowKey == SentinelRowKey)
                {
                    newETagValue = resultEntity.ETag;
                    break;
                }
            }

            this.settings.Logger.AppendedInstanceHistory(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                executionId,
                historyEventBatch.Count , // exclude sentinel from count
                numberOfTotalEvents,
                historyEventNamesBuffer.ToString(0, historyEventNamesBuffer.Length - 1), // remove trailing comma
                episodeNumber,
                resultInfo.ElapsedMilliseconds,
                estimatedBatchSizeInBytes,
                string.Concat(eTagValue ?? "(null)", " --> ", newETagValue ?? "(null)"),
                isFinalBatch);

            return newETagValue;
        }

        bool ExceedsMaxTablePropertySize(string data)
        {
            if (!string.IsNullOrEmpty(data) && Encoding.Unicode.GetByteCount(data) > MaxTablePropertySizeInBytes)
            {
                return true;
            }

            return false;
        }
    }
}
