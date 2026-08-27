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

namespace DurableTask.AzureStorage.Tracking
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Net;
    using System.Reflection;
    using System.Runtime.CompilerServices;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Data.Tables;
    using DurableTask.AzureStorage.Linq;
    using DurableTask.AzureStorage.Messaging;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.Core;
    using DurableTask.Core.History;

    /// <summary>
    /// Tracking store for use with <see cref="AzureStorageOrchestrationService"/>. Uses Azure Tables and Azure Blobs to store runtime state.
    /// </summary>
    class AzureTableTrackingStore : TrackingStoreBase
    {
        const string NameProperty = "Name";
        const string InputProperty = "Input";
        const string ResultProperty = "Result";
        const string OutputProperty = "Output";
        const string RowKeyProperty = nameof(ITableEntity.RowKey);
        const string PartitionKeyProperty = nameof(ITableEntity.PartitionKey);
        const string TimestampProperty = nameof(ITableEntity.Timestamp);
        const string SentinelRowKey = "sentinel";
        const string IsCheckpointCompleteProperty = "IsCheckpointComplete";
        const string CheckpointCompletedTimestampProperty = "CheckpointCompletedTimestamp";
        const string SequenceNumberProperty = "SequenceNumber";

        // Well-known partition/row key and column for the single-row durable migration marker.
        const string MigrationMarkerPartitionKey = "";
        const string MigrationMarkerRowKey = "";
        const string MigrationStateProperty = "State";

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
            "Correlation",
            "FailureDetails",
            "Tags",
        };

        readonly string storageAccountName;
        readonly string taskHubName;
        readonly AzureStorageClient azureStorageClient;
        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly AzureStorageOrchestrationServiceStats stats;
        readonly IReadOnlyDictionary<EventType, Type> eventTypeMap;
        readonly MessageManager messageManager;
        readonly ModifiedInstancesQueue modifiedInstancesQueue;
        readonly Table migrationTable;

        // The live-migration mode supplied at startup, or null when not migrating. Set once before dispatch begins.
        MigrationMode? migrationMode;

        public AzureTableTrackingStore(
            AzureStorageClient azureStorageClient,
            MessageManager messageManager,
            ModifiedInstancesQueue modifiedInstancesQueue)
        {
            this.azureStorageClient = azureStorageClient;
            this.messageManager = messageManager;
            this.modifiedInstancesQueue = modifiedInstancesQueue;
            this.settings = this.azureStorageClient.Settings;
            this.stats = this.azureStorageClient.Stats;
            this.taskHubName = settings.TaskHubName;

            this.storageAccountName = this.azureStorageClient.TableAccountName;

            string historyTableName = settings.HistoryTableName;
            string instancesTableName = settings.InstanceTableName;

            this.HistoryTable = this.azureStorageClient.GetTableReference(historyTableName);
            this.InstancesTable = this.azureStorageClient.GetTableReference(instancesTableName);

            this.migrationTable = this.azureStorageClient.GetTableReference(settings.MigrationTableName);

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
            Table instancesTable)
        {
            this.stats = stats;
            this.InstancesTable = instancesTable;
            this.settings = new AzureStorageOrchestrationServiceSettings
            {
                // Have to set FetchLargeMessageDataEnabled to false, as no MessageManager is 
                // instantiated for this test.
                FetchLargeMessageDataEnabled = false,
            };
        }

        internal Table HistoryTable { get; }

        internal Table InstancesTable { get; }

        /// <inheritdoc />
        public override Task CreateAsync(CancellationToken cancellationToken = default)
        {
            return Task.WhenAll(new Task[]
            {
                this.HistoryTable.CreateIfNotExistsAsync(cancellationToken),
                this.InstancesTable.CreateIfNotExistsAsync(cancellationToken)
            });
        }

        /// <inheritdoc />
        public override Task DeleteAsync(CancellationToken cancellationToken = default)
        {
            return Task.WhenAll(new Task[]
            {
                this.HistoryTable.DeleteIfExistsAsync(cancellationToken),
                this.InstancesTable.DeleteIfExistsAsync(cancellationToken)
            });
        }

        /// <inheritdoc />
        public override async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
        {
            return this.HistoryTable != null && this.InstancesTable != null && await this.HistoryTable.ExistsAsync(cancellationToken) && await this.InstancesTable.ExistsAsync(cancellationToken);
        }

        /// <inheritdoc />
        public override async Task<OrchestrationHistory> GetHistoryEventsAsync(string instanceId, string expectedExecutionId, CancellationToken cancellationToken = default)
        {
            TableQueryResults<TableEntity> results = await this
                .GetHistoryEntitiesResponseInfoAsync(instanceId, expectedExecutionId, null, cancellationToken)
                .GetResultsAsync(cancellationToken: cancellationToken);

            // The sentinel row should always be the last row
            TableEntity sentinel = results.Entities.LastOrDefault(e => e.RowKey == SentinelRowKey);

            IList<HistoryEvent> historyEvents;
            string executionId;
            TrackingStoreContext trackingStoreContext = new TrackingStoreContext();

            // If expectedExecutionId is provided but it does not match the sentinel executionId,
            // it may belong to a previous generation. In that case, treat it as an unknown executionId
            // and skip loading history.
            if (results.Entities.Count > 0 && (expectedExecutionId == null ||
                                               expectedExecutionId == sentinel?.GetString("ExecutionId")))
            {
                // The most recent generation will always be in the first history event.
                executionId = sentinel?.GetString("ExecutionId") ?? results.Entities[0].GetString("ExecutionId");

                // Convert the table entities into history events.
                var events = new List<HistoryEvent>(results.Entities.Count);

                foreach (TableEntity entity in results.Entities)
                {
                    if (entity.GetString("ExecutionId") != executionId)
                    {
                        // The remaining entities are from a previous generation and can be discarded.
                        break;
                    }

                    // The sentinel row does not contain any history events, so ignore and continue
                    if (entity == sentinel)
                    {
                        continue;
                    }

                    // Some entity properties may be stored in blob storage.
                    await this.DecompressLargeEntityProperties(entity, trackingStoreContext.Blobs, cancellationToken);

                    events.Add((HistoryEvent)TableEntityConverter.Deserialize(entity, GetTypeForTableEntity(entity)));
                }

                historyEvents = events;
            }
            else
            {
                historyEvents = Array.Empty<HistoryEvent>();
                executionId = expectedExecutionId;
            }

            // Read the checkpoint completion time from the sentinel row.
            // A sentinel won't exist only if no instance of this ID has ever existed or the instance history
            // was purged. The IsCheckpointCompleteProperty was newly added _after_ v1.6.4.
            DateTime checkpointCompletionTime = DateTime.MinValue;
            ETag? eTagValue = sentinel?.ETag;
            if (sentinel != null &&
                sentinel.TryGetValue(CheckpointCompletedTimestampProperty, out object timestampObj) &&
                timestampObj is DateTimeOffset timestampProperty)
            {
                checkpointCompletionTime = timestampProperty.DateTime;
            }

            int currentEpisodeNumber = Utils.GetEpisodeNumber(historyEvents);

            this.settings.Logger.FetchedInstanceHistory(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                executionId,
                historyEvents.Count,
                currentEpisodeNumber,
                results.RequestCount,
                results.ElapsedMilliseconds,
                eTagValue?.ToString(),
                checkpointCompletionTime,
                string.Join(",", historyEvents.Skip(Math.Max(0, historyEvents.Count - 10)).Select(e => e.EventType.ToString())));

            return new OrchestrationHistory(historyEvents, checkpointCompletionTime, eTagValue, trackingStoreContext);
        }

        TableQueryResponse<TableEntity> GetHistoryEntitiesResponseInfoAsync(string instanceId, string expectedExecutionId, IList<string> projectionColumns, CancellationToken cancellationToken)
        {
            string filter = AzureTableQueryFilter.PartitionKeyEquals(instanceId);
            if (!string.IsNullOrEmpty(expectedExecutionId))
            {
                // Use parameterized filters to prevent OData injection via crafted execution IDs
                string sentinelCondition = AzureTableQueryFilter.ColumnEquals(nameof(ITableEntity.RowKey), SentinelRowKey);
                string executionIdCondition = AzureTableQueryFilter.ColumnEquals(nameof(OrchestrationInstance.ExecutionId), expectedExecutionId);
                filter += $" and ({sentinelCondition} or {executionIdCondition})";
            }

            return this.HistoryTable.ExecuteQueryAsync<TableEntity>(filter, select: projectionColumns, cancellationToken: cancellationToken);
        }

        async Task<IReadOnlyList<TableEntity>> QueryHistoryAsync(string filter, string instanceId, CancellationToken cancellationToken)
        {
            TableQueryResults<TableEntity> results = await this
                .HistoryTable.ExecuteQueryAsync<TableEntity>(filter, cancellationToken: cancellationToken)
                .GetResultsAsync(cancellationToken: cancellationToken);

            IReadOnlyList<TableEntity> entities = results.Entities;

            string executionId = entities.FirstOrDefault()?.GetString(nameof(OrchestrationInstance.ExecutionId)) ?? string.Empty;
            this.settings.Logger.FetchedInstanceHistory(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                executionId,
                entities.Count,
                episode: -1, // We don't have enough information to get the episode number. It's also not important to have for this particular trace.
                results.RequestCount,
                results.ElapsedMilliseconds,
                eTag: string.Empty,
                DateTime.MinValue,
                string.Join(",", entities.Skip(Math.Max(0, entities.Count - 10)).Select(e => e.GetString(nameof(HistoryEvent.EventType)))));

            return entities;
        }

        public override async IAsyncEnumerable<string> RewindHistoryAsync(
            string instanceId,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
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
            string partitionFilter = AzureTableQueryFilter.PartitionKeyEquals(instanceId);

            string orchestratorStartedFilter = $"{partitionFilter} and {nameof(HistoryEvent.EventType)} eq '{nameof(EventType.OrchestratorStarted)}'";
            IReadOnlyList<TableEntity> orchestratorStartedEntities = await this.QueryHistoryAsync(orchestratorStartedFilter, instanceId, cancellationToken);

            // get most recent orchestratorStarted event
            string recentStartRowKey = orchestratorStartedEntities.Max(x => x.RowKey);
            var recentStartRow = orchestratorStartedEntities.Where(y => y.RowKey == recentStartRowKey).ToList();
            string executionId = recentStartRow[0].GetString(nameof(OrchestrationInstance.ExecutionId));
            DateTime instanceTimestamp = recentStartRow[0].Timestamp.GetValueOrDefault().DateTime;

            // Use parameterized filter to prevent OData injection via crafted execution IDs
            string executionIdFilter = AzureTableQueryFilter.ColumnEquals(nameof(OrchestrationInstance.ExecutionId), executionId);

            // Rewind changes history, so during a live migration the sentinel's sequence number must be bumped to match
            // the instance row update below. Compute it up front so it can be stamped on the sentinel row in-line while
            // clearing failure events, avoiding a separate write.
            string sanitizedInstanceId = KeySanitation.EscapePartitionKey(instanceId);
            long? newSequenceNumber = null;
            if (this.IsMigrationActive)
            {
                newSequenceNumber = await this.GetNextSequenceNumberAsync(sanitizedInstanceId, cancellationToken);
                // Rewind targets the latest execution, so no specific execution ID is available here.
                await this.modifiedInstancesQueue.AddInstanceAsync(
                    instanceId,
                    executionId: null,
                    newSequenceNumber.Value,
                    cancellationToken);
            }

            var updateFilterBuilder = new StringBuilder();
            updateFilterBuilder.Append($"{partitionFilter}");
            updateFilterBuilder.Append($" and {executionIdFilter}");
            updateFilterBuilder.Append(" and (");
            updateFilterBuilder.Append($"{nameof(ExecutionCompletedEvent.OrchestrationStatus)} eq '{nameof(OrchestrationStatus.Failed)}'");
            updateFilterBuilder.Append($" or {nameof(HistoryEvent.EventType)} eq '{nameof(EventType.TaskFailed)}'");
            updateFilterBuilder.Append($" or {nameof(HistoryEvent.EventType)} eq '{nameof(EventType.SubOrchestrationInstanceFailed)}'");
            if (this.IsMigrationActive)
            {
                // Include the sentinel row so its sequence number can be bumped in-line below.
                updateFilterBuilder.Append($" or {AzureTableQueryFilter.ColumnEquals(nameof(ITableEntity.RowKey), SentinelRowKey)}");
            }
            updateFilterBuilder.Append(')');

            IReadOnlyList<TableEntity> entitiesToClear = await this.QueryHistoryAsync(updateFilterBuilder.ToString(), instanceId, cancellationToken);
            foreach (TableEntity entity in entitiesToClear)
            {
                if (entity.GetString(nameof(OrchestrationInstance.ExecutionId)) != executionId)
                {
                    // the remaining entities are from a previous generation and can be discarded.
                    break;
                }

                if (entity.RowKey == SentinelRowKey)
                {
                    if (newSequenceNumber.HasValue)
                    {
                        entity[SequenceNumberProperty] = newSequenceNumber.Value;
                        await this.HistoryTable.ReplaceEntityAsync(entity, entity.ETag, cancellationToken);
                    }

                    continue;
                }

                int? taskScheduledId = entity.GetInt32(nameof(TaskCompletedEvent.TaskScheduledId));

                var eventFilterBuilder = new StringBuilder();
                eventFilterBuilder.Append($"{partitionFilter}");
                eventFilterBuilder.Append($" and {executionIdFilter}");
                eventFilterBuilder.Append($" and {nameof(HistoryEvent.EventId)} eq {taskScheduledId.GetValueOrDefault()}");

                switch (entity.GetString(nameof(HistoryEvent.EventType)))
                {
                    // delete TaskScheduled corresponding to TaskFailed event
                    case nameof(EventType.TaskFailed):
                        eventFilterBuilder.Append($" and {nameof(HistoryEvent.EventType)} eq '{nameof(EventType.TaskScheduled)}'");
                        IReadOnlyList<TableEntity> taskScheduledEntities = await this.QueryHistoryAsync(eventFilterBuilder.ToString(), instanceId, cancellationToken);

                        TableEntity tsEntity = taskScheduledEntities[0];
                        tsEntity[nameof(TaskFailedEvent.Reason)] = "Rewound: " + tsEntity.GetString(nameof(HistoryEvent.EventType));
                        tsEntity[nameof(TaskFailedEvent.EventType)] = nameof(EventType.GenericEvent);
                        await this.HistoryTable.ReplaceEntityAsync(tsEntity, tsEntity.ETag, cancellationToken);
                        break;

                    // delete SubOrchestratorCreated corresponding to SubOrchestraionInstanceFailed event
                    case nameof(EventType.SubOrchestrationInstanceFailed):
                        hasFailedSubOrchestrations = true;

                        eventFilterBuilder.Append($" and {nameof(HistoryEvent.EventType)} eq '{nameof(EventType.SubOrchestrationInstanceCreated)}'");
                        IReadOnlyList<TableEntity> subOrchesratrationEntities = await this.QueryHistoryAsync(eventFilterBuilder.ToString(), instanceId, cancellationToken);

                        // the SubOrchestrationCreatedEvent is still healthy and will not be overwritten, just marked as rewound
                        TableEntity soEntity = subOrchesratrationEntities[0];
                        soEntity[nameof(SubOrchestrationInstanceFailedEvent.Reason)] = "Rewound: " + soEntity.GetString(nameof(HistoryEvent.EventType));
                        await this.HistoryTable.ReplaceEntityAsync(soEntity, soEntity.ETag, cancellationToken);

                        // recursive call to clear out failure events on child instances
                        await foreach (string childInstanceId in this.RewindHistoryAsync(soEntity.GetString(nameof(OrchestrationInstance.InstanceId)), cancellationToken))
                        {
                            yield return childInstanceId;
                        }

                        break;
                }

                // "clear" failure event by making RewindEvent: replay ignores row while dummy event preserves rowKey
                entity[nameof(TaskFailedEvent.Reason)] = "Rewound: " + entity.GetString(nameof(HistoryEvent.EventType));
                entity[nameof(TaskFailedEvent.EventType)] = nameof(EventType.GenericEvent);

                await this.HistoryTable.ReplaceEntityAsync(entity, entity.ETag, cancellationToken);
            }

            // reset orchestration status in instance store table
            await this.UpdateStatusForRewindAsync(instanceId, newSequenceNumber, cancellationToken);

            if (!hasFailedSubOrchestrations)
            {
                yield return instanceId;
            }
        }

        /// <inheritdoc />
        public override async IAsyncEnumerable<OrchestrationState> GetStateAsync(string instanceId, bool allExecutions, bool fetchInput, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            InstanceStatus instanceStatus = await this.FetchInstanceStatusInternalAsync(instanceId, fetchInput, cancellationToken);
            if (instanceStatus != null)
            {
                yield return instanceStatus.State;
            }
        }
#nullable enable
        /// <inheritdoc />
        public override async Task<OrchestrationState?> GetStateAsync(string instanceId, string executionId, bool fetchInput, CancellationToken cancellationToken = default)
        {
            InstanceStatus? instanceStatus = await this.FetchInstanceStatusInternalAsync(instanceId, fetchInput, cancellationToken);
            return instanceStatus?.State;
        }

        /// <inheritdoc />
        public override Task<InstanceStatus?> FetchInstanceStatusAsync(string instanceId, CancellationToken cancellationToken = default)
        {
            return this.FetchInstanceStatusInternalAsync(instanceId, fetchInput: false, cancellationToken);
        }

        /// <inheritdoc />
        internal async Task<InstanceStatus?> FetchInstanceStatusInternalAsync(string instanceId, bool fetchInput, CancellationToken cancellationToken)
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

            ODataCondition odata = queryCondition.ToOData();

            var sw = Stopwatch.StartNew();

            OrchestrationInstanceStatus? tableEntity = await this.InstancesTable
                .ExecuteQueryAsync<OrchestrationInstanceStatus>(odata.Filter, 1, odata.Select, cancellationToken)
                .FirstOrDefaultAsync();

            sw.Stop();

            OrchestrationState? orchestrationState = tableEntity != null ? await this.ConvertFromAsync(tableEntity, cancellationToken) : null;

            this.settings.Logger.FetchedInstanceStatus(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                orchestrationState?.OrchestrationInstance.ExecutionId ?? string.Empty,
                orchestrationState?.OrchestrationStatus.ToString() ?? "NotFound",
                sw.ElapsedMilliseconds);

            if (tableEntity == null || orchestrationState == null)
            {
                return null;
            }

            return new InstanceStatus(orchestrationState, tableEntity.ETag, tableEntity.SequenceNumber);
        }
#nullable disable
        Task<OrchestrationState> ConvertFromAsync(OrchestrationInstanceStatus tableEntity, CancellationToken cancellationToken)
        {
            var instanceId = KeySanitation.UnescapePartitionKey(tableEntity.PartitionKey);
            return ConvertFromAsync(tableEntity, instanceId, cancellationToken);
        }

        async Task<OrchestrationState> ConvertFromAsync(OrchestrationInstanceStatus orchestrationInstanceStatus, string instanceId, CancellationToken cancellationToken)
        {
            var orchestrationState = new OrchestrationState();
            if (!Enum.TryParse(orchestrationInstanceStatus.RuntimeStatus, out orchestrationState.OrchestrationStatus))
            {
                // This is not expected, but could happen if there is invalid data in the Instances table, or if this is a tombstone
                // for a purged row when migration is active
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
            orchestrationState.Generation = orchestrationInstanceStatus.Generation;
            orchestrationState.Tags = !string.IsNullOrEmpty(orchestrationInstanceStatus.Tags)
                ? TagsSerializer.Deserialize(orchestrationInstanceStatus.Tags)
                : null;

            if (this.settings.FetchLargeMessageDataEnabled)
            {
                if (MessageManager.TryGetLargeMessageReference(orchestrationState.Input, out Uri blobUrl))
                {
                    string json = await this.messageManager.DownloadAndDecompressAsBytesAsync(blobUrl, cancellationToken);

                    // Depending on which blob this is, we interpret it differently.
                    if (blobUrl.AbsolutePath.EndsWith("ExecutionStarted.json.gz"))
                    {
                        // The downloaded content is an ExecutedStarted message payload that
                        // was created when the orchestration was started.
                        MessageData msg = this.messageManager.DeserializeMessageData(json);
                        if (msg?.TaskMessage?.Event is ExecutionStartedEvent startEvent)
                        {
                            orchestrationState.Input = startEvent.Input;
                        }
                        else
                        {
                            this.settings.Logger.GeneralWarning(
                                this.storageAccountName,
                                this.taskHubName,
                                $"Orchestration input blob URL '{blobUrl}' contained unrecognized data.",
                                instanceId);
                        }
                    }
                    else
                    {
                        // The downloaded content is the raw input JSON
                        orchestrationState.Input = json;
                    }
                }

                orchestrationState.Output = await this.messageManager.FetchLargeMessageIfNecessary(orchestrationState.Output, cancellationToken);
            }

            return orchestrationState;
        }

        /// <inheritdoc />
        public override async IAsyncEnumerable<InstanceStatus> FetchInstanceStatusAsync(IEnumerable<string> instanceIds, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (instanceIds == null)
            {
                yield break;
            }

            IEnumerable<Task<InstanceStatus>> instanceQueries = instanceIds.Select(instance => this.FetchInstanceStatusAsync(instance, cancellationToken));
            foreach (InstanceStatus status in await Task.WhenAll(instanceQueries))
            {
                if (status != null)
                {
                    yield return status;
                }
            }
        }

        /// <inheritdoc />
        public override IAsyncEnumerable<OrchestrationState> GetStateAsync(CancellationToken cancellationToken = default)
        {
            return this.QueryStateAsync($"{nameof(ITableEntity.RowKey)} eq ''", cancellationToken: cancellationToken);
        }

        public override AsyncPageable<OrchestrationState> GetStateAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus, CancellationToken cancellationToken = default)
        {
            ODataCondition odata = OrchestrationInstanceStatusQueryCondition.Parse(createdTimeFrom, createdTimeTo, runtimeStatus).ToOData();
            return this.QueryStateAsync(odata.Filter, odata.Select, cancellationToken);
        }

        public override AsyncPageable<OrchestrationState> GetStateAsync(OrchestrationInstanceStatusQueryCondition condition, CancellationToken cancellationToken = default)
        {
            ODataCondition odata = condition.ToOData();
            return this.QueryStateAsync(odata.Filter, odata.Select, cancellationToken);
        }

        AsyncPageable<OrchestrationState> QueryStateAsync(string filter = null, IEnumerable<string> select = null, CancellationToken cancellationToken = default)
        {
            return this.InstancesTable
                .ExecuteQueryAsync<OrchestrationInstanceStatus>(filter, select: select, cancellationToken: cancellationToken)
                .TransformPagesAsync((p, t) => p.Values
                    .SelectAsync((s, t) => new ValueTask<OrchestrationState>(this.ConvertFromAsync(s, KeySanitation.UnescapePartitionKey(s.PartitionKey), t))));
        }

        async Task<PurgeHistoryResult> DeleteHistoryAsync(
            DateTime createdTimeFrom,
            DateTime? createdTimeTo,
            IEnumerable<OrchestrationStatus> runtimeStatus,
            CancellationToken cancellationToken)
        {
            var condition = OrchestrationInstanceStatusQueryCondition.Parse(
                createdTimeFrom,
                createdTimeTo,
                runtimeStatus);
            condition.FetchInput = false;
            condition.FetchOutput = false;

            ODataCondition odata = condition.ToOData();

            int storageRequests = 0;
            int instancesDeleted = 0;
            int rowsDeleted = 0;

            CancellationToken effectiveToken = cancellationToken;

            // Limit concurrent instance purges to a fraction of the global storage concurrency budget.
            // This ensures purge operations don't starve normal orchestration processing (dispatch,
            // checkpoint, etc.) which shares the same global HTTP throttle. Each instance purge
            // internally spawns multiple parallel storage operations (history query + parallel batch
            // deletes + blob cleanup + instance row delete), so the effective storage pressure is
            // a multiple of this value. Using 1/3 of MaxStorageOperationConcurrency as a reasonable
            // upper bound that balances purge throughput against headroom for other operations.
            int maxPurgeConcurrency = Math.Max(1, this.settings.MaxStorageOperationConcurrency / 3);
            using var throttle = new SemaphoreSlim(maxPurgeConcurrency);
            var pendingTasks = new List<Task>();

            bool timedOut = false;
            int failedDeletes = 0;

            try
            {
                AsyncPageable<OrchestrationInstanceStatus> entitiesPageable = this.InstancesTable.ExecuteQueryAsync<OrchestrationInstanceStatus>(odata.Filter, select: odata.Select, cancellationToken: effectiveToken);
                await foreach (Page<OrchestrationInstanceStatus> page in entitiesPageable.AsPages(pageSizeHint: 100))
                {
                    foreach (OrchestrationInstanceStatus instance in page.Values)
                    {
                        effectiveToken.ThrowIfCancellationRequested();

                        await throttle.WaitAsync(effectiveToken);

                        async Task DeleteInstanceAsync(OrchestrationInstanceStatus inst)
                        {
                            string instanceId = KeySanitation.UnescapePartitionKey(inst.PartitionKey);
                            try
                            {
                                if (this.IsMigrationActive)
                                {
                                    long expectedSequenceNumber = (inst.SequenceNumber ?? 0) + 1;
                                    await this.modifiedInstancesQueue.AddInstanceAsync(
                                        instanceId,
                                        inst.ExecutionId,
                                        expectedSequenceNumber,
                                        effectiveToken);
                                }

                                PurgeHistoryResult statisticsFromDeletion = await this.DeleteAllDataForOrchestrationInstance(inst, effectiveToken);
                                Interlocked.Add(ref instancesDeleted, statisticsFromDeletion.InstancesDeleted);
                                Interlocked.Add(ref storageRequests, statisticsFromDeletion.StorageRequests);
                                Interlocked.Add(ref rowsDeleted, statisticsFromDeletion.RowsDeleted);
                            }
                            catch (Exception ex) when (ex is not OperationCanceledException)
                            {
                                // Log the failure but don't let a single instance failure crash the
                                // entire purge. The instance will remain and can be retried on the
                                // next purge call.
                                this.settings.Logger.GeneralWarning(
                                    this.storageAccountName,
                                    this.taskHubName,
                                    $"Failed to purge instance '{instanceId}': {ex.Message}",
                                    instanceId);
                                Interlocked.Increment(ref failedDeletes);
                            }
                            finally
                            {
                                throttle.Release();
                            }
                        }

                        pendingTasks.Add(DeleteInstanceAsync(instance));
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Cancellation requested (timeout or caller cancellation) — stop accepting new instances.
                timedOut = true;
            }

            // Wait for all remaining dispatched deletions to finish or be cancelled.
            try
            {
                await Task.WhenAll(pendingTasks);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // In-flight deletes were cancelled — expected.
                timedOut = true;
            }

            // When no cancellation/timeout semantics were requested (CancellationToken.None
            // in the back-compat path), preserve the legacy behavior by leaving IsComplete unset.
            // Otherwise, report false when timed out or when any individual instance deletions failed
            // (the failed instances remain and can be retried on the next purge call).
            bool? isComplete = cancellationToken.CanBeCanceled
                ? !timedOut && failedDeletes == 0
                : (bool?)null;

            return new PurgeHistoryResult(storageRequests, instancesDeleted, rowsDeleted, isComplete);
        }

        async Task<PurgeHistoryResult> DeleteAllDataForOrchestrationInstance(OrchestrationInstanceStatus orchestrationInstanceStatus, CancellationToken cancellationToken)
        {
            int storageRequests = 0;
            int rowsDeleted = 0;

            string sanitizedInstanceId = KeySanitation.UnescapePartitionKey(orchestrationInstanceStatus.PartitionKey);

            TableQueryResults<TableEntity> results = await this
                .GetHistoryEntitiesResponseInfoAsync(
                    instanceId: sanitizedInstanceId,
                    expectedExecutionId: null,
                    projectionColumns: new[] { RowKeyProperty, PartitionKeyProperty, TimestampProperty },
                    cancellationToken)
                .GetResultsAsync(cancellationToken: cancellationToken);

            storageRequests += results.RequestCount;

            IReadOnlyList<TableEntity> historyEntities = results.Entities;

            // During a live migration, a purge must leave the instance row behind with an incremented sequence
            // number (all other data is deleted) so the migration process can observe the purge.
            TableEntity purgedInstanceEntity = null;
            if (this.IsMigrationActive)
            {
                // The status entity already carries the current sequence number for both callers: the single-instance
                // purge reads the full row, and the date-range purge's projection is derived from the entity's own
                // properties (it only omits Input/Output), so SequenceNumber is always included.
                long newSequenceNumber = (orchestrationInstanceStatus.SequenceNumber ?? 0) + 1;
                purgedInstanceEntity = new TableEntity(orchestrationInstanceStatus.PartitionKey, string.Empty)
                {
                    [SequenceNumberProperty] = newSequenceNumber,
                    // Retain the execution ID in case the orchestration is recreated so that OrchestrationSessionManager.DedupeExecutionStartedMessagesAsync does not
                    // allow the ExecutionStartedMessage to go through before the instance table is updated with the new instance
                    ["ExecutionId"] = orchestrationInstanceStatus.ExecutionId,
                };

            }

            var tasks = new List<Task>
            {
                Task.Run(async () =>
                {
                    int storageOperations = await this.messageManager.DeleteLargeMessageBlobs(sanitizedInstanceId, cancellationToken);
                    Interlocked.Add(ref storageRequests, storageOperations);
                }),
                Task.Run(async () =>
                {
                    var deletedEntitiesResponseInfo = await this.HistoryTable.DeleteBatchParallelAsync(historyEntities, cancellationToken);
                    Interlocked.Add(ref rowsDeleted, deletedEntitiesResponseInfo.Responses.Count);
                    Interlocked.Add(ref storageRequests, deletedEntitiesResponseInfo.RequestCount);
                })
            };

            if (!this.IsMigrationActive)
            {
                tasks.Add(this.InstancesTable.DeleteEntityAsync(
                    new TableEntity(orchestrationInstanceStatus.PartitionKey, string.Empty),
                    ETag.All,
                    cancellationToken: cancellationToken));
            }

            await Task.WhenAll(tasks);

            if (purgedInstanceEntity is not null)
            {
                // Write the tombstone row last so that the new sequence number only becomes visible
                // once all other data has been deleted
                await this.InstancesTable.InsertOrReplaceEntityAsync(purgedInstanceEntity, cancellationToken);
            }

            // This is for the instances table deletion
            storageRequests++;

            return new PurgeHistoryResult(storageRequests, 1, rowsDeleted);
        }

        /// <inheritdoc />
        public override Task PurgeHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public override async Task<PurgeHistoryResult> PurgeInstanceHistoryAsync(
            string instanceId,
            CancellationToken cancellationToken = default)
        {
            // Use parameterized filters to prevent OData injection via crafted instance IDs
            string filter = $"{AzureTableQueryFilter.PartitionKeyEquals(instanceId)} and {AzureTableQueryFilter.ColumnEquals(RowKeyProperty, string.Empty)}";
            var results = await this.InstancesTable
                .ExecuteQueryAsync<OrchestrationInstanceStatus>(filter, cancellationToken: cancellationToken)
                .GetResultsAsync(cancellationToken: cancellationToken);

            OrchestrationInstanceStatus orchestrationInstanceStatus = results.Entities.FirstOrDefault();

            if (orchestrationInstanceStatus != null)
            {
                if (this.IsMigrationActive)
                {
                    long expectedSequenceNumber = (orchestrationInstanceStatus.SequenceNumber ?? 0) + 1;
                    // Single-instance purge is by instance ID only, so no specific execution ID is available here.
                    await this.modifiedInstancesQueue.AddInstanceAsync(
                        instanceId,
                        executionId: null,
                        expectedSequenceNumber,
                        cancellationToken);
                }

                PurgeHistoryResult result = await this.DeleteAllDataForOrchestrationInstance(orchestrationInstanceStatus, cancellationToken);

                this.settings.Logger.PurgeInstanceHistory(
                    this.storageAccountName,
                    this.taskHubName,
                    instanceId,
                    DateTime.MinValue.ToString(),
                    DateTime.MinValue.ToString(),
                    string.Empty,
                    result.StorageRequests,
                    result.InstancesDeleted,
                    results.ElapsedMilliseconds);

                return result;
            }

            return new PurgeHistoryResult(0, 0, 0);
        }

        /// <inheritdoc />
        public override async Task<PurgeHistoryResult> PurgeInstanceHistoryAsync(
            DateTime createdTimeFrom,
            DateTime? createdTimeTo,
            IEnumerable<OrchestrationStatus> runtimeStatus,
            CancellationToken cancellationToken = default)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            List<OrchestrationStatus> runtimeStatusList = runtimeStatus?.Where(
               status => status == OrchestrationStatus.Completed ||
                    status == OrchestrationStatus.Terminated ||
                    status == OrchestrationStatus.Canceled ||
                    status == OrchestrationStatus.Failed).ToList();

            PurgeHistoryResult result = await this.DeleteHistoryAsync(createdTimeFrom, createdTimeTo, runtimeStatusList, cancellationToken);

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
            ETag? eTag,
            string inputPayloadOverride,
            long sequenceNumber,
            CancellationToken cancellationToken = default)
        {
            string sanitizedInstanceId = KeySanitation.EscapePartitionKey(executionStartedEvent.OrchestrationInstance.InstanceId);
            TableEntity entity = new TableEntity(sanitizedInstanceId, "")
            {
                ["Input"] = inputPayloadOverride ?? executionStartedEvent.Input,
                ["CreatedTime"] = executionStartedEvent.Timestamp,
                ["Name"] = executionStartedEvent.Name,
                ["Version"] = executionStartedEvent.Version,
                ["RuntimeStatus"] = OrchestrationStatus.Pending.ToString("G"),
                ["LastUpdatedTime"] = DateTime.UtcNow,
                ["TaskHubName"] = this.settings.TaskHubName,
                ["ScheduledStartTime"] = executionStartedEvent.ScheduledStartTime,
                ["ExecutionId"] = executionStartedEvent.OrchestrationInstance.ExecutionId,
                ["Generation"] = executionStartedEvent.Generation,
                ["Tags"] = TagsSerializer.Serialize(executionStartedEvent.Tags),
            };

            long expectedSequenceNumber = sequenceNumber + 1;
            if (this.IsMigrationActive)
            {
                await this.modifiedInstancesQueue.AddInstanceAsync(
                    executionStartedEvent.OrchestrationInstance.InstanceId,
                    executionStartedEvent.OrchestrationInstance.ExecutionId,
                    expectedSequenceNumber,
                    cancellationToken);
            }

            // It is possible that the queue message was small enough to be written directly to a queue message,
            // not a blob, but is too large to be written to a table property.
            await this.CompressLargeMessageAsync(entity, listOfBlobs: null, cancellationToken: cancellationToken);

            // Overwriting an existing row (a recreate, or a purged instance whose row we kept with only a sequence
            // number) replaces the whole row, so carry forward the existing sequence number, incremented.
            if (this.IsMigrationActive)
            {
                entity[SequenceNumberProperty] = expectedSequenceNumber;
            }

            Stopwatch stopwatch = Stopwatch.StartNew();
            try
            {
                if (eTag == null)
                {
                    // This is the case for creating a new instance.
                    await this.InstancesTable.InsertEntityAsync(entity, cancellationToken);
                }
                else
                {
                    // This is the case for overwriting an existing instance.
                    await this.InstancesTable.ReplaceEntityAsync(entity, eTag.GetValueOrDefault(), cancellationToken);
                }
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
        public override async Task UpdateStatusForRewindAsync(string instanceId, long? sequenceNumber, CancellationToken cancellationToken = default)
        {
            string sanitizedInstanceId = KeySanitation.EscapePartitionKey(instanceId);
            TableEntity entity = new TableEntity(sanitizedInstanceId, "")
            {
                ["RuntimeStatus"] = OrchestrationStatus.Pending.ToString("G"),
                ["LastUpdatedTime"] = DateTime.UtcNow,
            };

            if (sequenceNumber.HasValue)
            {
                entity[SequenceNumberProperty] = sequenceNumber.Value;
            }

            Stopwatch stopwatch = Stopwatch.StartNew();
            await this.InstancesTable.MergeEntityAsync(entity, ETag.All, cancellationToken);

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
        public override async Task UpdateStatusForTerminationAsync(
            string instanceId,
            ExecutionTerminatedEvent executionTerminatedEvent,
            long sequenceNumber,
            CancellationToken cancellationToken = default)
        {
            string sanitizedInstanceId = KeySanitation.EscapePartitionKey(instanceId);
            TableEntity instanceEntity = new TableEntity(sanitizedInstanceId, "")
            {
                ["RuntimeStatus"] = OrchestrationStatus.Terminated.ToString("G"),
                ["LastUpdatedTime"] = executionTerminatedEvent.Timestamp,
                ["CompletedTime"] = DateTime.UtcNow,
                // In the case of terminating an orchestration, the termination reason becomes the orchestration's output.
                [OutputProperty] = executionTerminatedEvent.Input,
            };

            if (this.IsMigrationActive)
            {
                await this.modifiedInstancesQueue.AddInstanceAsync(
                    instanceId,
                    executionId: null,
                    sequenceNumber,
                    cancellationToken);
            }

            // Setting addBlobPropertyName to false ensures that the blob URL is saved as the "Output" of the instance entity, which is the expected behavior
            // for large orchestration outputs.
            await this.CompressLargeMessageAsync(instanceEntity, listOfBlobs: null, cancellationToken: cancellationToken, addBlobPropertyName: false);

            if (this.IsMigrationActive)
            {
                instanceEntity[SequenceNumberProperty] = sequenceNumber;
            }

            Stopwatch stopwatch = Stopwatch.StartNew();
            await this.InstancesTable.MergeEntityAsync(instanceEntity, ETag.All, cancellationToken);

            this.settings.Logger.InstanceStatusUpdate(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                string.Empty,
                OrchestrationStatus.Terminated,
                episode: 0,
                stopwatch.ElapsedMilliseconds);
        }


        /// <inheritdoc />
        public override Task StartAsync(CancellationToken cancellationToken = default)
        {
            ServicePointManager.FindServicePoint(this.HistoryTable.Uri).UseNagleAlgorithm = false;
            ServicePointManager.FindServicePoint(this.InstancesTable.Uri).UseNagleAlgorithm = false;

            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public override bool IsMigrationActive => this.migrationMode == MigrationMode.MigrationStarted;

        /// <inheritdoc />
        public override async Task SetMigrationModeAsync(MigrationMode mode, CancellationToken cancellationToken = default)
        {
            if (mode == MigrationMode.MigrationEnding)
            {
                await this.RecordMigrationEndingMarkerAsync(cancellationToken);
            }

            this.migrationMode = mode;
        }

        async Task RecordMigrationEndingMarkerAsync(CancellationToken cancellationToken)
        {
            await this.migrationTable.CreateIfNotExistsAsync(cancellationToken);

            var marker = new TableEntity(MigrationMarkerPartitionKey, MigrationMarkerRowKey)
            {
                [MigrationStateProperty] = MigrationMode.MigrationEnding.ToString(),
            };

            await this.migrationTable.InsertOrReplaceEntityAsync(marker, cancellationToken);
        }

        /// <summary>
        /// Returns the next per-instance sequence number to persist on the instance and history tables while a
        /// migration is active. Enqueuing the instance into the modified-instances queue is the responsibility of
        /// the caller (the orchestration service), which does so before invoking the write.
        /// </summary>
        /// <returns>The next sequence number (current value + 1, or 1 if not yet set).</returns>
        async Task<long> GetNextSequenceNumberAsync(string sanitizedInstanceId, CancellationToken cancellationToken)
        {
            long? currentSequenceNumber = await this.GetInstanceSequenceNumberAsync(sanitizedInstanceId, cancellationToken);
            return (currentSequenceNumber ?? 0) + 1;
        }

        async Task<long?> GetInstanceSequenceNumberAsync(string sanitizedInstanceId, CancellationToken cancellationToken)
        {
            string filter = $"{AzureTableQueryFilter.PartitionKeyEquals(sanitizedInstanceId)} and {AzureTableQueryFilter.ColumnEquals(RowKeyProperty, string.Empty)}";
            TableQueryResults<TableEntity> results = await this.InstancesTable
                .ExecuteQueryAsync<TableEntity>(filter, select: new[] { SequenceNumberProperty }, cancellationToken: cancellationToken)
                .GetResultsAsync(cancellationToken: cancellationToken);

            TableEntity entity = results.Entities.FirstOrDefault();
            if (entity != null && entity.TryGetValue(SequenceNumberProperty, out object value) && value is long sequenceNumber)
            {
                return sequenceNumber;
            }

            return null;
        }

        /// <inheritdoc />
        public override async Task UpdateStateAsync(
            OrchestrationRuntimeState newRuntimeState,
            OrchestrationRuntimeState oldRuntimeState,
            string instanceId,
            string executionId,
            OrchestrationConcurrencyTags concurrencyTags,
            object trackingStoreContext,
            CancellationToken cancellationToken = default)
        {
            int estimatedBytes = 0;
            IList<HistoryEvent> newEvents = newRuntimeState.NewEvents;
            IList<HistoryEvent> allEvents = newRuntimeState.Events;
            TrackingStoreContext context = (TrackingStoreContext)trackingStoreContext;

            int episodeNumber = Utils.GetEpisodeNumber(newRuntimeState);

            var newEventListBuffer = new StringBuilder(4000);
            var historyEventBatch = new List<TableTransactionAction>();

            OrchestrationStatus runtimeStatus = OrchestrationStatus.Running;
            string sanitizedInstanceId = KeySanitation.EscapePartitionKey(instanceId);

            var instanceEntity = new TableEntity(sanitizedInstanceId, string.Empty)
            {
                // TODO: Translating null to "null" is a temporary workaround. We should prioritize 
                // https://github.com/Azure/durabletask/issues/477 so that this is no longer necessary.
                ["CustomStatus"] = newRuntimeState.Status ?? "null",
                ["ExecutionId"] = executionId,
                ["LastUpdatedTime"] = newEvents.Last().Timestamp,
                ["TaskHubName"] = this.settings.TaskHubName,
            };

            // If a live migration is in progress, bump this instance's per-instance sequence number. The same value
            // is written to both the instance table (below) and the history table's sentinel row (in
            // UploadHistoryBatch) so the two can be reconciled.
            if (this.IsMigrationActive)
            {
                concurrencyTags.InstanceSequenceNumber =
                    concurrencyTags.InstanceSequenceNumber.GetValueOrDefault() + 1;
                instanceEntity[SequenceNumberProperty] = concurrencyTags.InstanceSequenceNumber;
                await this.modifiedInstancesQueue.AddInstanceAsync(
                    instanceId,
                    executionId,
                    concurrencyTags.InstanceSequenceNumber.Value,
                    cancellationToken);
            }

            // check if we are replacing a previous execution with blobs; those will be deleted from the store after the update. This could occur in a ContinueAsNew scenario
            List<string> blobsToDelete = null;
            if (oldRuntimeState != newRuntimeState && context.Blobs.Count > 0)
            {
                blobsToDelete = context.Blobs;
                context.Blobs = new List<string>();
            }

            for (int i = 0; i < newEvents.Count; i++)
            {
                bool isFinalEvent = i == newEvents.Count - 1;

                HistoryEvent historyEvent = newEvents[i];
                // For backwards compatibility, we convert timer timestamps to UTC prior to persisting to Azure Storage
                // see: https://github.com/Azure/durabletask/pull/1138
                Utils.ConvertDateTimeInHistoryEventsToUTC(historyEvent);
                var historyEntity = TableEntityConverter.Serialize(historyEvent);
                historyEntity.PartitionKey = sanitizedInstanceId;

                newEventListBuffer.Append(historyEvent.EventType.ToString()).Append(',');

                // The row key is the sequence number, which represents the chronological ordinal of the event.
                long sequenceNumber = i + (allEvents.Count - newEvents.Count);
                historyEntity.RowKey = sequenceNumber.ToString("X16");
                historyEntity["ExecutionId"] = executionId;

                await this.CompressLargeMessageAsync(historyEntity, context.Blobs, cancellationToken);

                // Replacement can happen if the orchestration episode gets replayed due to a commit failure in one of the steps below.
                historyEventBatch.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, historyEntity));

                // Keep track of the byte count to ensure we don't hit the 4 MB per-batch maximum
                estimatedBytes += GetEstimatedByteCount(historyEntity);

                // Monitor for orchestration instance events 
                switch (historyEvent.EventType)
                {
                    case EventType.ExecutionStarted:
                        runtimeStatus = OrchestrationStatus.Running;
                        ExecutionStartedEvent executionStartedEvent = (ExecutionStartedEvent)historyEvent;
                        instanceEntity["Name"] = executionStartedEvent.Name;
                        instanceEntity["Version"] = executionStartedEvent.Version;
                        instanceEntity["CreatedTime"] = executionStartedEvent.Timestamp;
                        instanceEntity["RuntimeStatus"] = OrchestrationStatus.Running.ToString();
                        instanceEntity["Tags"] = TagsSerializer.Serialize(executionStartedEvent.Tags);
                        instanceEntity["Generation"] = executionStartedEvent.Generation;
                        if (executionStartedEvent.ScheduledStartTime.HasValue)
                        {
                            instanceEntity["ScheduledStartTime"] = executionStartedEvent.ScheduledStartTime;
                        }

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
                        instanceEntity["RuntimeStatus"] = executionCompleted.OrchestrationStatus.ToString();
                        instanceEntity["CompletedTime"] = DateTime.UtcNow;
                        this.SetInstancesTablePropertyFromHistoryProperty(
                            historyEntity,
                            instanceEntity,
                            historyPropertyName: nameof(executionCompleted.Result),
                            instancePropertyName: OutputProperty,
                            data: executionCompleted.FailureDetails?.ToString() ?? executionCompleted.Result);
                        break;
                    case EventType.ExecutionTerminated:
                        runtimeStatus = OrchestrationStatus.Terminated;
                        ExecutionTerminatedEvent executionTerminatedEvent = (ExecutionTerminatedEvent)historyEvent;
                        instanceEntity["RuntimeStatus"] = OrchestrationStatus.Terminated.ToString();
                        instanceEntity["CompletedTime"] = DateTime.UtcNow;
                        this.SetInstancesTablePropertyFromHistoryProperty(
                            historyEntity,
                            instanceEntity,
                            historyPropertyName: nameof(executionTerminatedEvent.Input),
                            instancePropertyName: OutputProperty,
                            data: executionTerminatedEvent.Input);
                        break;
                    case EventType.ExecutionSuspended:
                        runtimeStatus = OrchestrationStatus.Suspended;
                        ExecutionSuspendedEvent executionSuspendedEvent = (ExecutionSuspendedEvent)historyEvent;
                        instanceEntity["RuntimeStatus"] = OrchestrationStatus.Suspended.ToString();
                        this.SetInstancesTablePropertyFromHistoryProperty(
                            historyEntity,
                            instanceEntity,
                            historyPropertyName: nameof(executionSuspendedEvent.Reason),
                            instancePropertyName: OutputProperty,
                            data: executionSuspendedEvent.Reason);
                        break;
                    case EventType.ExecutionResumed:
                        runtimeStatus = OrchestrationStatus.Running;
                        ExecutionResumedEvent executionResumedEvent = (ExecutionResumedEvent)historyEvent;
                        instanceEntity["RuntimeStatus"] = OrchestrationStatus.Running.ToString();
                        this.SetInstancesTablePropertyFromHistoryProperty(
                            historyEntity,
                            instanceEntity,
                            historyPropertyName: nameof(executionResumedEvent.Reason),
                            instancePropertyName: OutputProperty,
                            data: executionResumedEvent.Reason);
                        break;
                    case EventType.ContinueAsNew:
                        runtimeStatus = OrchestrationStatus.ContinuedAsNew;
                        ExecutionCompletedEvent executionCompletedEvent = (ExecutionCompletedEvent)historyEvent;
                        instanceEntity["RuntimeStatus"] = OrchestrationStatus.ContinuedAsNew.ToString();
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
                    concurrencyTags.HistoryETag = await this.UploadHistoryBatch(
                        instanceId,
                        sanitizedInstanceId,
                        executionId,
                        historyEventBatch,
                        newEventListBuffer,
                        allEvents.Count,
                        episodeNumber,
                        estimatedBytes,
                        concurrencyTags.HistoryETag,
                        concurrencyTags.InstanceSequenceNumber,
                        isFinalBatch: isFinalEvent,
                        cancellationToken: cancellationToken);

                    // Reset local state for the next batch
                    newEventListBuffer.Clear();
                    historyEventBatch.Clear();
                    estimatedBytes = 0;
                }
            }

            // First persistence step is to commit history to the history table. Messages must come after.
            if (historyEventBatch.Count > 0)
            {
                concurrencyTags.HistoryETag = await this.UploadHistoryBatch(
                    instanceId,
                    sanitizedInstanceId,
                    executionId,
                    historyEventBatch,
                    newEventListBuffer,
                    allEvents.Count,
                    episodeNumber,
                    estimatedBytes,
                    concurrencyTags.HistoryETag,
                    concurrencyTags.InstanceSequenceNumber,
                    isFinalBatch: true,
                    cancellationToken: cancellationToken);
            }

            concurrencyTags.InstanceETag = await this.UpdateInstanceTableAsync(instanceEntity, concurrencyTags.InstanceETag, instanceId, executionId, runtimeStatus, episodeNumber);

            // finally, delete orphaned blobs from the previous execution history.
            // We had to wait until the new history has committed to make sure the blobs are no longer necessary.
            if (blobsToDelete != null)
            {
                var tasks = new List<Task>(blobsToDelete.Count);
                foreach (var blobName in blobsToDelete)
                {
                    tasks.Add(this.messageManager.DeleteBlobAsync(blobName));
                }
                await Task.WhenAll(tasks);
            }
        }

        public override async Task UpdateInstanceStatusForCompletedOrchestrationAsync(
            string instanceId,
            string executionId,
            OrchestrationRuntimeState runtimeState,
            bool instanceEntityExists,
            long sequenceNumber,
            CancellationToken cancellationToken = default)
        {
            if (runtimeState.OrchestrationStatus != OrchestrationStatus.Completed &&
                runtimeState.OrchestrationStatus != OrchestrationStatus.Canceled &&
                runtimeState.OrchestrationStatus != OrchestrationStatus.Failed &&
                runtimeState.OrchestrationStatus != OrchestrationStatus.Terminated)
            {
                return;
            }

            if (this.IsMigrationActive)
            {
                await this.modifiedInstancesQueue.AddInstanceAsync(
                    instanceId,
                    executionId,
                    sequenceNumber,
                    cancellationToken);
            }

            string sanitizedInstanceId = KeySanitation.EscapePartitionKey(instanceId);
            ExecutionStartedEvent executionStartedEvent = runtimeState.ExecutionStartedEvent;

            // We need to set all of the fields of the instance entity in the case that it was never created for the orchestration.
            // This can be the case for a suborchestration that completed in one execution, for example.
            var instanceEntity = new TableEntity(sanitizedInstanceId, string.Empty)
            {
                ["Name"] = runtimeState.Name,
                ["Version"] = runtimeState.Version,
                ["CreatedTime"] = executionStartedEvent.Timestamp,
                // TODO: Translating null to "null" is a temporary workaround. We should prioritize 
                // https://github.com/Azure/durabletask/issues/477 so that this is no longer necessary.
                ["CustomStatus"] = runtimeState.Status ?? "null",
                ["ExecutionId"] = executionId,
                ["LastUpdatedTime"] = runtimeState.Events.Last().Timestamp,
                ["RuntimeStatus"] = runtimeState.OrchestrationStatus.ToString(),
                ["CompletedTime"] = runtimeState.CompletedTime,
                ["Tags"] = TagsSerializer.Serialize(executionStartedEvent.Tags),
                ["TaskHubName"] = this.settings.TaskHubName,
            };
            if (runtimeState.ExecutionStartedEvent.ScheduledStartTime.HasValue)
            {
                instanceEntity["ScheduledStartTime"] = executionStartedEvent.ScheduledStartTime;
            }

            static TableEntity GetSingleEntityFromHistoryTableResults(IReadOnlyList<TableEntity> entities, string dataType)
            {
                try
                {
                    TableEntity singleEntity = entities.SingleOrDefault();

                    return singleEntity ?? throw new DurableTaskStorageException($"The history table query to determine the blob storage URL " +
                        $"for the large orchestration {dataType} returned no rows. Unable to extract the URL from these results.");
                }
                catch (InvalidOperationException)
                {
                    throw new DurableTaskStorageException($"The history table query to determine the blob storage URL for the large orchestration " +
                        $"{dataType} returned more than one row, when exactly one row is expected. " +
                        $"Unable to extract the URL from these results.");
                }
            }

            // Set the output.
            // In the case that the output is too large and is stored in blob storage, extract the blob name from the ExecutionCompleted history entity.
            if (this.ExceedsMaxTablePropertySize(runtimeState.Output))
            {
                // Use parameterized filters to prevent OData injection via crafted instance/execution IDs
                string filter = $"{AzureTableQueryFilter.PartitionKeyEquals(instanceId)}" +
                    $" and {AzureTableQueryFilter.ColumnEquals(nameof(OrchestrationInstance.ExecutionId), executionId)}" +
                    $" and {AzureTableQueryFilter.ColumnEquals(nameof(HistoryEvent.EventType), nameof(EventType.ExecutionCompleted))}";
                TableEntity executionCompletedEntity = GetSingleEntityFromHistoryTableResults(await this.QueryHistoryAsync(filter, instanceId, cancellationToken), "output");
                this.SetInstancesTablePropertyFromHistoryProperty(
                    executionCompletedEntity,
                    instanceEntity,
                    historyPropertyName: nameof(runtimeState.ExecutionCompletedEvent.Result),
                    instancePropertyName: OutputProperty,
                    data: runtimeState.Output);
            }
            else
            {
                instanceEntity[OutputProperty] = runtimeState.Output;
            }
            
            // If the input has not been set by a previous execution, set the input.
            if (!instanceEntityExists)
            {
                // In the case that the input is too large and is stored in blob storage, extract the blob name from the ExecutionStarted history entity.
                if (this.ExceedsMaxTablePropertySize(runtimeState.Input))
                {
                    // Use parameterized filters to prevent OData injection via crafted instance/execution IDs
                    string filter = $"{AzureTableQueryFilter.PartitionKeyEquals(instanceId)}" +
                        $" and {AzureTableQueryFilter.ColumnEquals(nameof(OrchestrationInstance.ExecutionId), executionId)}" +
                        $" and {AzureTableQueryFilter.ColumnEquals(nameof(HistoryEvent.EventType), nameof(EventType.ExecutionStarted))}";
                    TableEntity executionStartedEntity = GetSingleEntityFromHistoryTableResults(await this.QueryHistoryAsync(filter, instanceId, cancellationToken), "input");
                    this.SetInstancesTablePropertyFromHistoryProperty(
                        executionStartedEntity,
                        instanceEntity,
                        historyPropertyName: nameof(executionStartedEvent.Input),
                        instancePropertyName: InputProperty,
                        data: executionStartedEvent.Input);
                }
                else
                {
                    instanceEntity[InputProperty] = runtimeState.Input;
                }
            }

            Stopwatch orchestrationInstanceUpdateStopwatch = Stopwatch.StartNew();

            // During a migration, persist the incremented sequence number supplied by the caller (or 1 if the instance
            // had none) so the instance row stays consistent with the history sentinel.
            if (this.IsMigrationActive)
            {
                instanceEntity[SequenceNumberProperty] = sequenceNumber;
            }

            await this.InstancesTable.InsertOrMergeEntityAsync(instanceEntity);

            this.settings.Logger.InstanceStatusUpdate(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                executionId,
                runtimeState.OrchestrationStatus,
                Utils.GetEpisodeNumber(runtimeState),
                orchestrationInstanceUpdateStopwatch.ElapsedMilliseconds);
        }

        static int GetEstimatedByteCount(TableEntity entity)
        {
            // Assume at least 1 KB of data per entity to account for static-length properties
            int estimatedByteCount = 1024;

            // Count the bytes for variable-length properties, which are assumed to always be strings
            foreach (string propertyName in VariableSizeEntityProperties)
            {
                if (entity.TryGetValue(propertyName, out object property) && property is string stringProperty && stringProperty != "")
                {
                    estimatedByteCount += Encoding.Unicode.GetByteCount(stringProperty);
                }
            }

            return estimatedByteCount;
        }

        Type GetTypeForTableEntity(TableEntity tableEntity)
        {
            string propertyName = nameof(HistoryEvent.EventType);

            if (!tableEntity.TryGetValue(propertyName, out object eventTypeProperty))
            {
                throw new ArgumentException($"The TableEntity did not contain a '{propertyName}' property.");
            }

            if (eventTypeProperty is not string stringProperty)
            {
                throw new ArgumentException($"The TableEntity's {propertyName} property type must a String.");
            }

            if (!Enum.TryParse(stringProperty, out EventType eventType))
            {
                throw new ArgumentException($"{stringProperty} is not a valid EventType value.");
            }

            return this.eventTypeMap[eventType];
        }

        // Assigns the target table entity property. Any large message for type 'Input, or 'Output' would have been compressed earlier as part of the 'entity' object,
        // so, we only need to assign the 'entity' object's blobName to the target table entity blob name property.
        void SetInstancesTablePropertyFromHistoryProperty(
            TableEntity TableEntity,
            TableEntity instanceEntity,
            string historyPropertyName,
            string instancePropertyName,
            string data)
        {
            string blobPropertyName = GetBlobPropertyName(historyPropertyName);
            if (TableEntity.TryGetValue(blobPropertyName, out object blobProperty) && blobProperty is string blobName)
            {
                // This is a large message
                string blobUrl = this.messageManager.GetBlobUrl(blobName);
                instanceEntity[instancePropertyName] = blobUrl;
            }
            else
            {
                // This is a normal-sized message and can be stored inline
                instanceEntity[instancePropertyName] = data;
            }
        }

        async Task CompressLargeMessageAsync(TableEntity entity, List<string> listOfBlobs, CancellationToken cancellationToken, bool addBlobPropertyName = true)
        {
            foreach (string propertyName in VariableSizeEntityProperties)
            {
                if (entity.TryGetValue(propertyName, out object property) &&
                    property is string stringProperty &&
                    this.ExceedsMaxTablePropertySize(stringProperty))
                {
                    // Upload the large property as a blob in Blob Storage since it won't fit in table storage.
                    string blobName = GetBlobName(entity, propertyName);
                    byte[] messageBytes = Encoding.UTF8.GetBytes(stringProperty);
                    await this.messageManager.CompressAndUploadAsBytesAsync(messageBytes, blobName, cancellationToken);

                    // Clear out the original property value and create a new "*BlobName"-suffixed property.
                    // The runtime will look for the new "*BlobName"-suffixed column to know if a property is stored in a blob.
                    if (addBlobPropertyName)
                    {
                        string blobPropertyName = GetBlobPropertyName(propertyName);
                        entity.Add(blobPropertyName, blobName);
                        entity[propertyName] = string.Empty;
                    }
                    else
                    {
                        entity[propertyName] = this.messageManager.GetBlobUrl(blobName);
                    }

                    // if necessary, keep track of all the blobs associated with this execution
                    listOfBlobs?.Add(blobName);
                }
            }
        }

        async Task DecompressLargeEntityProperties(TableEntity entity, List<string> listOfBlobs, CancellationToken cancellationToken)
        {
            // Check for entity properties stored in blob storage
            foreach (string propertyName in VariableSizeEntityProperties)
            {
                string blobPropertyName = GetBlobPropertyName(propertyName);
                if (entity.TryGetValue(blobPropertyName, out object property) && property is string blobName)
                {
                    string decompressedMessage = await this.messageManager.DownloadAndDecompressAsBytesAsync(blobName, cancellationToken);
                    entity[propertyName] = decompressedMessage;
                    entity.Remove(blobPropertyName);

                    // keep track of all the blobs associated with this execution
                    listOfBlobs.Add(blobName);
                }
            }
        }

        static string GetBlobPropertyName(string originalPropertyName)
        {
            // WARNING: Changing this is a breaking change!
            return originalPropertyName + "BlobName";
        }

        static string GetBlobName(TableEntity entity, string property)
        {
            string sanitizedInstanceId = entity.PartitionKey;
            string sequenceNumber = entity.RowKey;

            string eventType;
            if (entity.TryGetValue("EventType", out object obj) && obj is string value)
            {
                eventType = value;
            }
            else if (property == "Input")
            {
                // This message is just to start the orchestration, so it does not have a corresponding
                // EventType. Use a hardcoded value to record the orchestration input.
                eventType = "Input";
            }
            else if (property == "Output")
            {
                // This message is used to terminate an orchestration with no history, so it does not have a
                // corresponding EventType. Use a hardcoded value to record the orchestration output.
                eventType = "Output";
            }
            else if (property == "Tags")
            {
                eventType = "Tags";
            }
            else
            {
                throw new InvalidOperationException($"Could not compute the blob name for property {property}");
            }

            // randomize the blob name to prevent accidental races in split-brain situations (#890)
            uint random = (uint)(new Random()).Next();

            return $"{sanitizedInstanceId}/history-{sequenceNumber}-{eventType}-{random:X8}-{property}.json.gz";
        }

        async Task<ETag?> UploadHistoryBatch(
            string instanceId,
            string sanitizedInstanceId,
            string executionId,
            IList<TableTransactionAction> historyEventBatch,
            StringBuilder historyEventNamesBuffer,
            int numberOfTotalEvents,
            int episodeNumber,
            int estimatedBatchSizeInBytes,
            ETag? eTagValue,
            long? migrationSequenceNumber,
            bool isFinalBatch,
            CancellationToken cancellationToken)
        {
            // Adding / updating sentinel entity
            TableEntity sentinelEntity = new TableEntity(sanitizedInstanceId, SentinelRowKey)
            {
                ["ExecutionId"] = executionId,
                [IsCheckpointCompleteProperty] = isFinalBatch,
            };

            if (isFinalBatch)
            {
                sentinelEntity[CheckpointCompletedTimestampProperty] = DateTime.UtcNow;
            }

            // During a live migration, stamp the sentinel row with the same per-instance sequence number written to
            // the instance table so that the two tables can be reconciled by the migration process.
            if (migrationSequenceNumber.HasValue)
            {
                sentinelEntity[SequenceNumberProperty] = migrationSequenceNumber.Value;
            }

            if (eTagValue != null)
            {
                historyEventBatch.Add(new TableTransactionAction(TableTransactionActionType.UpdateMerge, sentinelEntity, eTagValue.GetValueOrDefault()));
            }
            else
            {
                historyEventBatch.Add(new TableTransactionAction(TableTransactionActionType.Add, sentinelEntity));
            }

            TableTransactionResults resultInfo;
            Stopwatch stopwatch = Stopwatch.StartNew();
            try
            {
                resultInfo = await this.HistoryTable.ExecuteBatchAsync(historyEventBatch, cancellationToken);
            }
            catch (DurableTaskStorageException ex)
            {
                // Handle the case where the history has already been updated by another caller.
                // Common case: the resulting code is 'PreconditionFailed', which means "eTagValue" no longer matches the one stored, and TableTransactionActionType is "Update".
                // Edge case: the resulting code is 'Conflict'. This is the case when eTagValue is null, and the TableTransactionActionType is "Add",
                // in which case the exception indicates that the table entity we are trying to "add" already exists.
                if (ex.HttpStatusCode == (int)HttpStatusCode.Conflict || ex.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
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
                        eTagValue is null ? string.Empty : eTagValue.ToString());
                }

                throw;
            }

            IReadOnlyList<Response> responses = resultInfo.Responses;
            ETag? newETagValue = null;
            for (int i = responses.Count - 1; i >= 0; i--)
            {
                if (historyEventBatch[i].Entity.RowKey == SentinelRowKey)
                {
                    newETagValue = responses[i].Headers.ETag;
                    break;
                }
            }

            this.settings.Logger.AppendedInstanceHistory(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                executionId,
                historyEventBatch.Count - 1, // exclude sentinel from count
                numberOfTotalEvents,
                historyEventNamesBuffer.ToString(0, historyEventNamesBuffer.Length - 1), // remove trailing comma
                episodeNumber,
                resultInfo.ElapsedMilliseconds,
                estimatedBatchSizeInBytes,
                string.Concat(eTagValue?.ToString() ?? "(null)", " --> ", newETagValue?.ToString() ?? "(null)"),
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

        async Task<ETag?> UpdateInstanceTableAsync(TableEntity instanceEntity, ETag? eTag, string instanceId, string executionId, OrchestrationStatus runtimeStatus, int episodeNumber)
        {
            var orchestrationInstanceUpdateStopwatch = Stopwatch.StartNew();

            ETag? newEtag = null;

            if (!this.settings.UseInstanceTableEtag && !this.IsMigrationActive)
            {
                await this.InstancesTable.InsertOrMergeEntityAsync(instanceEntity);
            }
            else
            {
                try
                {
                    Response result = await (eTag == null
                        ? this.InstancesTable.InsertEntityAsync(instanceEntity)
                        : this.InstancesTable.MergeEntityAsync(instanceEntity, eTag.Value));
                    newEtag = result.Headers.ETag;
                }
                catch (DurableTaskStorageException ex)
                {
                    // Handle the case where the instance table has already been updated by another caller.
                    // Common case: the resulting code is 'PreconditionFailed', which means we are trying to update an existing instance entity and "eTag" no longer matches the one stored.
                    // Edge case: the resulting code is 'Conflict'. This is the case when eTag is null, and we are trying to insert a new instance entity, in which case the exception
                    // indicates that the table entity we are trying to "add" already exists.
                    if (ex.HttpStatusCode == (int)HttpStatusCode.Conflict || ex.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                    {
                        this.settings.Logger.SplitBrainDetected(
                            this.storageAccountName,
                            this.taskHubName,
                            instanceId,
                            executionId,
                            newEventCount: 0,
                            totalEventCount: 1,
                            "InstanceEntity",
                            orchestrationInstanceUpdateStopwatch.ElapsedMilliseconds,
                            eTag is null ? string.Empty : eTag.ToString());
                    }

                    throw;
                }
            }

            this.settings.Logger.InstanceStatusUpdate(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                executionId,
                runtimeStatus,
                episodeNumber,
                orchestrationInstanceUpdateStopwatch.ElapsedMilliseconds);

            return newEtag;

        }

        class TrackingStoreContext
        {
            public List<string> Blobs { get; set; } = new List<string>();
        }
    }
}
