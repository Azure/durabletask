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
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Monitoring;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;

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

        // See https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-the-table-service-data-model#property-types
        const int MaxTablePropertySizeInBytes = 60 * 1024; // 60KB

        static readonly string[] VariableSizeEntityProperties = new[]
        {
            NameProperty,
            InputProperty,
            ResultProperty,
            OutputProperty,
            "Reason",
            "Details",
        };

        readonly string storageAccountName;
        readonly string taskHubName;
        readonly AzureStorageOrchestrationServiceSettings settings;
        readonly AzureStorageOrchestrationServiceStats stats;
        readonly TableEntityConverter tableEntityConverter;
        readonly IReadOnlyDictionary<EventType, Type> eventTypeMap;
        readonly MessageManager messageManager;

        public AzureTableTrackingStore(
            AzureStorageOrchestrationServiceSettings settings,
            MessageManager messageManager,
            AzureStorageOrchestrationServiceStats stats,
            CloudStorageAccount account)
        {
            this.settings = settings;
            this.messageManager = messageManager;
            this.stats = stats;
            this.tableEntityConverter = new TableEntityConverter();
            this.taskHubName = settings.TaskHubName;

            this.storageAccountName = account.Credentials.AccountName;

            CloudTableClient tableClient = account.CreateCloudTableClient();
            tableClient.BufferManager = SimpleBufferManager.Shared;

            string historyTableName = $"{taskHubName}History";
            NameValidator.ValidateTableName(historyTableName);

            string instancesTableName = $"{taskHubName}Instances";
            NameValidator.ValidateTableName(instancesTableName);

            this.HistoryTable = tableClient.GetTableReference(historyTableName);
            this.InstancesTable = tableClient.GetTableReference(instancesTableName);

            this.StorageTableRequestOptions = settings.HistoryTableRequestOptions;

            // Use reflection to learn all the different event types supported by DTFx.
            // This could have been hardcoded, but I generally try to avoid hardcoding of point-in-time DTFx knowledge.
            Type historyEventType = typeof(HistoryEvent);

            IEnumerable<Type> historyEventTypes = historyEventType.Assembly.GetTypes().Where(
                t => !t.IsAbstract && t.IsSubclassOf(historyEventType));

            PropertyInfo eventTypeProperty = historyEventType.GetProperty(nameof(HistoryEvent.EventType));
            this.eventTypeMap = historyEventTypes.ToDictionary(
                type => ((HistoryEvent)FormatterServices.GetUninitializedObject(type)).EventType);
        }

        internal AzureTableTrackingStore(
            AzureStorageOrchestrationServiceStats stats,
            CloudTable instancesTable
        )
        {
            this.stats = stats;
            this.InstancesTable = instancesTable;
        }

        /// <summary>
        ///  Table Request Options for The History and Instance Tables
        /// </summary>
        public TableRequestOptions StorageTableRequestOptions { get; set; }

        internal CloudTable HistoryTable { get; }

        internal CloudTable InstancesTable { get; }

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
            HistoryEntitiesResponseInfo historyEntitiesResponseInfo = await this.GetHistoryEntitiesResponseInfoAsync(instanceId, expectedExecutionId, null, cancellationToken);
            IList<HistoryEvent> historyEvents;
            string executionId;
            string eTagValue = null;
            if (historyEntitiesResponseInfo.HistoryEventEntities.Count > 0)
            {
                // The most recent generation will always be in the first history event.
                executionId = historyEntitiesResponseInfo.HistoryEventEntities[0].Properties["ExecutionId"].StringValue;

                // Convert the table entities into history events.
                var events = new List<HistoryEvent>(historyEntitiesResponseInfo.HistoryEventEntities.Count);

                foreach (DynamicTableEntity entity in historyEntitiesResponseInfo.HistoryEventEntities)
                {
                    if (entity.Properties["ExecutionId"].StringValue != executionId)
                    {
                        // The remaining entities are from a previous generation and can be discarded.
                        break;
                    }

                    if (entity.RowKey == SentinelRowKey)
                    {
                        eTagValue = entity.ETag;
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

            int currentEpisodeNumber = Utils.GetEpisodeNumber(historyEvents);

            AnalyticsEventSource.Log.FetchedInstanceHistory(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                executionId,
                historyEvents.Count,
                currentEpisodeNumber,
                historyEntitiesResponseInfo.RequestCount,
                historyEntitiesResponseInfo.ElapsedMilliseconds,
                eTagValue,
                Utils.ExtensionVersion);

            return new OrchestrationHistory(historyEvents, eTagValue);
        }

        async Task<HistoryEntitiesResponseInfo> GetHistoryEntitiesResponseInfoAsync(string instanceId, string expectedExecutionId, IList<string> projectionColumns,  CancellationToken cancellationToken = default(CancellationToken))
        {
            var filterCondition = new StringBuilder(200);

            const char Quote = '\'';

            // e.g. "PartitionKey eq 'c138dd969a1e4a699b0644c7d8279f81'"
            filterCondition.Append(PartitionKeyProperty).Append(" eq ").Append(Quote).Append(instanceId).Append(Quote);
            if (!string.IsNullOrEmpty(expectedExecutionId))
            {
                // Filter down to a specific generation.
                // e.g. "PartitionKey eq 'c138dd969a1e4a699b0644c7d8279f81' and ExecutionId eq '85f05ce1494c4a29989f64d3fe0f9089'"
                filterCondition.Append(" and ExecutionId eq ").Append(Quote).Append(expectedExecutionId).Append(Quote);
            }

            TableQuery query = new TableQuery().Where(filterCondition.ToString());

            if (projectionColumns != null)
            {
                query.Select(projectionColumns);
            }

            // TODO: Write-through caching should ensure that we rarely need to make this call?
            var historyEventEntities = new List<DynamicTableEntity>(100);

            var stopwatch = new Stopwatch();
            int requestCount = 0;
            long elapsedMilliseconds = 0;
            TableContinuationToken continuationToken = null;
            while (true)
            {
                stopwatch.Start();
                var segment = await this.HistoryTable.ExecuteQuerySegmentedAsync(
                    query,
                    continuationToken,
                    this.StorageTableRequestOptions,
                    null,
                    cancellationToken);
                stopwatch.Stop();
                elapsedMilliseconds += stopwatch.ElapsedMilliseconds;
                this.stats.StorageRequests.Increment();
                this.stats.TableEntitiesRead.Increment(segment.Results.Count);

                requestCount++;
                historyEventEntities.AddRange(segment);

                continuationToken = segment.ContinuationToken;
                if (continuationToken == null || cancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }

            return new HistoryEntitiesResponseInfo
            {
                HistoryEventEntities = historyEventEntities,
                ElapsedMilliseconds = elapsedMilliseconds,
                RequestCount = requestCount
            };
        }

        async Task<List<DynamicTableEntity>> QueryHistoryAsync(string filterCondition, string instanceId, CancellationToken cancellationToken)
        {
            TableQuery query = new TableQuery().Where(filterCondition.ToString());

            var entities = new List<DynamicTableEntity>(100);

            var stopwatch = new Stopwatch();
            int requestCount = 0;

            bool finishedEarly = false;
            TableContinuationToken continuationToken = null;
            while (true)
            {
                requestCount++;
                stopwatch.Start();
                var segment = await this.HistoryTable.ExecuteQuerySegmentedAsync(
                    query,
                    continuationToken,
                    this.StorageTableRequestOptions,
                    null,
                    cancellationToken);
                stopwatch.Stop();

                int previousCount = entities.Count;
                entities.AddRange(segment);
                this.stats.StorageRequests.Increment();
                this.stats.TableEntitiesRead.Increment(entities.Count - previousCount);

                continuationToken = segment.ContinuationToken;
                if (finishedEarly || continuationToken == null || cancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }

            // We don't have enough information to get the episode number.
            // It's also not important to have for this particular trace.
            int currentEpisodeNumber = 0;

            string executionId = entities.Count > 0 && entities.First().Properties.ContainsKey("ExecutionId") ?
                entities[0].Properties["ExecutionId"].StringValue :
                string.Empty;

            AnalyticsEventSource.Log.FetchedInstanceHistory(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                executionId,
                entities.Count,
                currentEpisodeNumber,
                requestCount,
                stopwatch.ElapsedMilliseconds,
                string.Empty /* ETag */,
                Utils.ExtensionVersion);

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

            string soInstanceId = instanceId;
            bool hasFailedSubOrchestrations = false;
            const char Quote = '\'';

            var orchestratorStartedFilterCondition = new StringBuilder(200);

            orchestratorStartedFilterCondition.Append(PartitionKeyProperty).Append(" eq ").Append(Quote).Append(instanceId).Append(Quote); // = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, instanceId);
            orchestratorStartedFilterCondition.Append(" and EventType eq ").Append(Quote).Append("OrchestratorStarted").Append(Quote);

            var orchestratorStartedEntities = await this.QueryHistoryAsync(orchestratorStartedFilterCondition.ToString(), instanceId, cancellationToken);

            // get most recent orchestratorStarted event
            var recentStartRowKey = orchestratorStartedEntities.Max(x => x.RowKey);
            var recentStartRow = orchestratorStartedEntities.Where(y => y.RowKey == recentStartRowKey).ToList();
            var executionId = recentStartRow[0].Properties["ExecutionId"].StringValue;
            var instanceTimestamp = recentStartRow[0].Timestamp.DateTime;

            var rowsToUpdateFilterCondition = new StringBuilder(200);

            rowsToUpdateFilterCondition.Append(PartitionKeyProperty).Append(" eq ").Append(Quote).Append(instanceId).Append(Quote);
            rowsToUpdateFilterCondition.Append(" and ExecutionId eq ").Append(Quote).Append(executionId).Append(Quote);
            rowsToUpdateFilterCondition.Append(" and (OrchestrationStatus eq ").Append(Quote).Append("Failed").Append(Quote);
            rowsToUpdateFilterCondition.Append(" or EventType eq").Append(Quote).Append("TaskFailed").Append(Quote);
            rowsToUpdateFilterCondition.Append(" or EventType eq").Append(Quote).Append("SubOrchestrationInstanceFailed").Append(Quote).Append(")");

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

                    tsFilterCondition.Append(PartitionKeyProperty).Append(" eq ").Append(Quote).Append(instanceId).Append(Quote);
                    tsFilterCondition.Append(" and ExecutionId eq ").Append(Quote).Append(executionId).Append(Quote);
                    tsFilterCondition.Append(" and EventId eq ").Append(taskScheduledId);
                    tsFilterCondition.Append(" and EventType eq ").Append(Quote).Append(nameof(EventType.TaskScheduled)).Append(Quote);

                    var taskScheduledEntities = await QueryHistoryAsync(tsFilterCondition.ToString(), instanceId, cancellationToken);

                    taskScheduledEntities[0].Properties["Reason"] = new EntityProperty("Rewound: " + taskScheduledEntities[0].Properties["EventType"].StringValue);
                    taskScheduledEntities[0].Properties["EventType"] = new EntityProperty(nameof(EventType.GenericEvent));
                    
                    await this.HistoryTable.ExecuteAsync(TableOperation.Replace(taskScheduledEntities[0]));
                }

                // delete SubOrchestratorCreated corresponding to SubOrchestraionInstanceFailed event
                if (entity.Properties["EventType"].StringValue == nameof(EventType.SubOrchestrationInstanceFailed))
                {
                    hasFailedSubOrchestrations = true;
                    var subOrchestrationId = entity.Properties["TaskScheduledId"].Int32Value.ToString();

                    var soFilterCondition = new StringBuilder(200);

                    soFilterCondition.Append(PartitionKeyProperty).Append(" eq ").Append(Quote).Append(instanceId).Append(Quote);
                    soFilterCondition.Append(" and ExecutionId eq ").Append(Quote).Append(executionId).Append(Quote);
                    soFilterCondition.Append(" and EventId eq ").Append(subOrchestrationId);
                    soFilterCondition.Append(" and EventType eq ").Append(Quote).Append(nameof(EventType.SubOrchestrationInstanceCreated)).Append(Quote);

                    var subOrchesratrationEntities = await QueryHistoryAsync(soFilterCondition.ToString(), instanceId, cancellationToken);

                    soInstanceId = subOrchesratrationEntities[0].Properties["InstanceId"].StringValue;

                    // the SubORchestrationCreatedEvent is still healthy and will not be overwritten, just marked as rewound
                    subOrchesratrationEntities[0].Properties["Reason"] = new EntityProperty("Rewound: " + subOrchesratrationEntities[0].Properties["EventType"].StringValue);

                    await this.HistoryTable.ExecuteAsync(TableOperation.Replace(subOrchesratrationEntities[0]));

                    // recursive call to clear out failure events on child instances
                    await this.RewindHistoryAsync(soInstanceId, failedLeaves, cancellationToken);
                }

                // "clear" failure event by making RewindEvent: replay ignores row while dummy event preserves rowKey
                entity.Properties["Reason"] = new EntityProperty("Rewound: " + entity.Properties["EventType"].StringValue);
                entity.Properties["EventType"] = new EntityProperty(nameof(EventType.GenericEvent));

                await this.HistoryTable.ExecuteAsync(TableOperation.Replace(entity));
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
            if (instanceId == null)
            {
                throw new ArgumentNullException(nameof(instanceId));
            }

            List<string> columnsToRetrieve = null; // Default of null => retrieve all columns
            if (!fetchInput)
            {
                columnsToRetrieve = typeof(OrchestrationInstanceStatus).GetProperties().Select(prop => prop.Name).ToList();
                if (columnsToRetrieve.Contains(InputProperty))
                {
                    // Retrieve all columns except the input column
                    columnsToRetrieve.Remove(InputProperty);
                }            
            }

            var stopwatch = new Stopwatch();
            TableResult orchestration = await this.InstancesTable.ExecuteAsync(TableOperation.Retrieve<OrchestrationInstanceStatus>(instanceId, "", columnsToRetrieve));
            stopwatch.Stop();
            this.stats.StorageRequests.Increment();
            this.stats.TableEntitiesRead.Increment(1);

            AnalyticsEventSource.Log.FetchedInstanceStatus(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                executionId ?? string.Empty,
                stopwatch.ElapsedMilliseconds,
                Utils.ExtensionVersion);

            OrchestrationInstanceStatus orchestrationInstanceStatus = (OrchestrationInstanceStatus)orchestration.Result;
            if (orchestrationInstanceStatus == null)
            {
                return null;
            }

            return this.ConvertFromAsync(orchestrationInstanceStatus, instanceId);
        }

        OrchestrationState ConvertFromAsync(OrchestrationInstanceStatus orchestrationInstanceStatus, string instanceId)
        {
            var orchestrationState = new OrchestrationState();
            if (!Enum.TryParse(orchestrationInstanceStatus.RuntimeStatus, out orchestrationState.OrchestrationStatus))
            {
                throw new ArgumentException($"{orchestrationInstanceStatus.RuntimeStatus} is not a valid OrchestrationStatus value.");
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
            orchestrationState.LastUpdatedTime = orchestrationInstanceStatus.LastUpdatedTime;
            orchestrationState.Input = orchestrationInstanceStatus.Input;
            orchestrationState.Output = orchestrationInstanceStatus.Output;

            return orchestrationState;
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

        async Task<DurableStatusQueryResult> QueryStateAsync(TableQuery<OrchestrationInstanceStatus> query, int top, string continuationToken, CancellationToken cancellationToken)
        {
            TableContinuationToken token = null;
            var orchestrationStates = new List<OrchestrationState>(top);
            if (!string.IsNullOrEmpty(continuationToken))
            {
                var tokenContent = Encoding.UTF8.GetString(Convert.FromBase64String(continuationToken));
                token = JsonConvert.DeserializeObject<TableContinuationToken>(tokenContent);
            }

            query.Take(top);
            var segment = await this.InstancesTable.ExecuteQuerySegmentedAsync(query, token);
            IEnumerable<OrchestrationState> result = segment.Select(status => this.ConvertFromAsync(status, status.PartitionKey));
            orchestrationStates.AddRange(result);

            this.stats.StorageRequests.Increment();
            this.stats.TableEntitiesRead.Increment(orchestrationStates.Count);

            token = segment.ContinuationToken;
            var tokenJson = JsonConvert.SerializeObject(token);
            return new DurableStatusQueryResult()
            {
                OrchestrationState = orchestrationStates,
                ContinuationToken = Convert.ToBase64String(Encoding.UTF8.GetBytes(tokenJson))
            };
        }

        async Task<IList<OrchestrationState>> QueryStateAsync(TableQuery<OrchestrationInstanceStatus> query, CancellationToken cancellationToken)
        {
            TableContinuationToken token = null;
            var orchestrationStates = new List<OrchestrationState>(100);
            while (true)
            {
                TableQuerySegment<OrchestrationInstanceStatus> segment = await this.InstancesTable.ExecuteQuerySegmentedAsync(query, token);

                int previousCount = orchestrationStates.Count;
                IEnumerable<OrchestrationState> result = segment.Select(
                    status => this.ConvertFromAsync(status, status.PartitionKey));
                orchestrationStates.AddRange(result);

                this.stats.StorageRequests.Increment();
                this.stats.TableEntitiesRead.Increment(orchestrationStates.Count - previousCount);

                token = segment.ContinuationToken;

                if (token == null || cancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }

            return orchestrationStates;
        }

        async Task<PurgeHistoryResult> DeleteHistoryAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus)
        {
            TableQuery<OrchestrationInstanceStatus> query = OrchestrationInstanceStatusQueryCondition.Parse(createdTimeFrom, createdTimeTo, runtimeStatus)
                .ToTableQuery<OrchestrationInstanceStatus>();

            TableContinuationToken token = null;
            var orchestrationStates = new List<OrchestrationInstanceStatus>(100);

            int storageRequests = 0;
            int rowsDeleted = 0;
            int instancesDeleted = 0;

            while (true)
            {
                TableQuerySegment<OrchestrationInstanceStatus> segment = await this.InstancesTable.ExecuteQuerySegmentedAsync(query, token);
                storageRequests++;
                this.stats.StorageRequests.Increment();
                this.stats.TableEntitiesRead.Increment(segment.Results.Count);
                instancesDeleted = segment.Results.Count;

                foreach (OrchestrationInstanceStatus orchestrationInstanceStatus in segment.Results)
                {
                    var statisticsFromDeletion = await this.DeleteAllDataForOrchestrationInstance(orchestrationInstanceStatus);
                    storageRequests += statisticsFromDeletion.StorageRequests;
                    rowsDeleted += statisticsFromDeletion.RowsDeleted;
                }
                orchestrationStates.AddRange(segment.Results);
                token = segment.ContinuationToken;
                if (token == null)
                {
                    break;
                }
            }

            return new PurgeHistoryResult(storageRequests, instancesDeleted, rowsDeleted);
        }

        async Task<PurgeHistoryResult> DeleteAllDataForOrchestrationInstance(OrchestrationInstanceStatus orchestrationInstanceStatus)
        {
            int storageRequests = 0;
            int rowsDeleted = 0;
            HistoryEntitiesResponseInfo historyEntitiesResponseInfo = await this.GetHistoryEntitiesResponseInfoAsync(
                orchestrationInstanceStatus.PartitionKey,
                null,
                new []
                {
                    RowKeyProperty
                });
            storageRequests += historyEntitiesResponseInfo.RequestCount;

            int pageOffset = 0;
            while (pageOffset < historyEntitiesResponseInfo.HistoryEventEntities.Count)
            {
                var batch = new TableBatchOperation();
                List<DynamicTableEntity> batchForDeletion = historyEntitiesResponseInfo.HistoryEventEntities.Skip(pageOffset).Take(100).ToList();
                rowsDeleted += batchForDeletion.Count;

                foreach (DynamicTableEntity itemForDeletion in batchForDeletion)
                {
                    batch.Delete(itemForDeletion);
                }

                await this.messageManager.DeleteLargeMessageBlobs(orchestrationInstanceStatus.PartitionKey, this.stats);
                await this.HistoryTable.ExecuteBatchAsync(batch);
                this.stats.TableEntitiesWritten.Increment(batch.Count);
                storageRequests++;
                pageOffset += batchForDeletion.Count;
            }

            await this.InstancesTable.ExecuteAsync(TableOperation.Delete(new DynamicTableEntity
            {
                PartitionKey = orchestrationInstanceStatus.PartitionKey,
                RowKey = string.Empty,
                ETag = "*"
            }));
            this.stats.TableEntitiesWritten.Increment();
            this.stats.StorageRequests.Increment();
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
            TableQuery<OrchestrationInstanceStatus> query = new TableQuery<OrchestrationInstanceStatus>().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition(PartitionKeyProperty, QueryComparisons.Equal, instanceId),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition(RowKeyProperty, QueryComparisons.Equal, string.Empty)));

            Stopwatch stopwatch = Stopwatch.StartNew();

            TableQuerySegment<OrchestrationInstanceStatus> segment = 
                await this.InstancesTable.ExecuteQuerySegmentedAsync(query, null);
            this.stats.TableEntitiesRead.Increment();
            this.stats.StorageRequests.Increment();

            OrchestrationInstanceStatus orchestrationInstanceStatus = segment?.Results?.FirstOrDefault();

            if (orchestrationInstanceStatus != null)
            {
                var deletionStats = await this.DeleteAllDataForOrchestrationInstance(orchestrationInstanceStatus);

                AnalyticsEventSource.Log.PurgeInstanceHistory(
                    this.storageAccountName,
                    this.taskHubName,
                    instanceId,
                    DateTime.MinValue.ToString(),
                    DateTime.MinValue.ToString(),
                    null,
                    deletionStats.StorageRequests,
                    stopwatch.ElapsedMilliseconds,
                    Utils.ExtensionVersion);

                return deletionStats;
            }

            return new PurgeHistoryResult(0, 0, 0);
        }

        /// <inheritdoc />
        public override async Task<PurgeHistoryResult> PurgeInstanceHistoryAsync(DateTime createdTimeFrom, DateTime? createdTimeTo, IEnumerable<OrchestrationStatus> runtimeStatus)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            List<OrchestrationStatus> runtimeStatusList =  runtimeStatus?.Where(
               x => x == OrchestrationStatus.Completed ||
                    x == OrchestrationStatus.Terminated ||
                    x == OrchestrationStatus.Canceled ||
                    x == OrchestrationStatus.Failed).ToList();

            var stats = await this.DeleteHistoryAsync(createdTimeFrom, createdTimeTo, runtimeStatusList);

            AnalyticsEventSource.Log.PurgeInstanceHistory(
                this.storageAccountName,
                this.taskHubName,
                string.Empty,
                createdTimeFrom.ToString(),
                createdTimeTo.ToString() ?? DateTime.MinValue.ToString(),
                runtimeStatus != null ?
                    string.Join(",", runtimeStatus.Select(x => x.ToString()).ToArray()) :
                    string.Empty,
                stats.StorageRequests,
                stopwatch.ElapsedMilliseconds,
                Utils.ExtensionVersion);

            return stats;
        }

        /// <inheritdoc />
        public override async Task<bool> SetNewExecutionAsync(
            ExecutionStartedEvent executionStartedEvent,
            bool ignoreExistingInstances,
            string inputStatusOverride)
        {
            DynamicTableEntity entity = new DynamicTableEntity(executionStartedEvent.OrchestrationInstance.InstanceId, "")
            {
                Properties =
                {
                    ["Input"] = new EntityProperty(inputStatusOverride ?? executionStartedEvent.Input),
                    ["CreatedTime"] = new EntityProperty(executionStartedEvent.Timestamp),
                    ["Name"] = new EntityProperty(executionStartedEvent.Name),
                    ["Version"] = new EntityProperty(executionStartedEvent.Version),
                    ["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.Pending.ToString()),
                    ["LastUpdatedTime"] = new EntityProperty(DateTime.UtcNow),
                }
            };

            Stopwatch stopwatch = Stopwatch.StartNew();
            try
            {
                if (ignoreExistingInstances)
                {
                    await this.InstancesTable.ExecuteAsync(TableOperation.Insert(entity));
                }
                else
                {
                    await this.InstancesTable.ExecuteAsync(TableOperation.InsertOrReplace(entity));
                }
            }
            catch (StorageException e) when (e.RequestInformation?.HttpStatusCode == 409)
            {
                // Ignore. The main scenario for this is handling race conditions in status update.
                return false;
            }
            finally
            {
                this.stats.StorageRequests.Increment();
            }

            this.stats.TableEntitiesWritten.Increment();

            // Episode 0 means the orchestrator hasn't started yet.
            int currentEpisodeNumber = 0;

            AnalyticsEventSource.Log.InstanceStatusUpdate(
                this.storageAccountName,
                this.taskHubName,
                executionStartedEvent.OrchestrationInstance.InstanceId,
                executionStartedEvent.OrchestrationInstance.ExecutionId,
                executionStartedEvent.EventType.ToString(),
                currentEpisodeNumber,
                stopwatch.ElapsedMilliseconds,
                Utils.ExtensionVersion);

            return true;
        }

        /// <inheritdoc />
        public override async Task UpdateStatusForRewindAsync(string instanceId)
        {
            DynamicTableEntity entity = new DynamicTableEntity(instanceId, "")
            {
                ETag = "*",
                Properties =
                {
                    ["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.Pending.ToString()),
                    ["LastUpdatedTime"] = new EntityProperty(DateTime.UtcNow),
                }
            };

            Stopwatch stopwatch = Stopwatch.StartNew();
            await this.InstancesTable.ExecuteAsync(TableOperation.Merge(entity));
            this.stats.StorageRequests.Increment();
            this.stats.TableEntitiesWritten.Increment(1);

            // We don't have enough information to get the episode number.
            // It's also not important to have for this particular trace.
            int currentEpisodeNumber = 0;

            AnalyticsEventSource.Log.InstanceStatusUpdate(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                string.Empty,
                nameof(EventType.GenericEvent),
                currentEpisodeNumber,
                stopwatch.ElapsedMilliseconds,
                Utils.ExtensionVersion);
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
            OrchestrationRuntimeState runtimeState,
            string instanceId,
            string executionId,
            string eTagValue)
        {
            int estimatedBytes = 0;
            IList<HistoryEvent> newEvents = runtimeState.NewEvents;
            IList<HistoryEvent> allEvents = runtimeState.Events;

            int episodeNumber = Utils.GetEpisodeNumber(runtimeState);

            var newEventListBuffer = new StringBuilder(4000);
            var historyEventBatch = new TableBatchOperation();

            EventType? orchestratorEventType = null;

            var instanceEntity = new DynamicTableEntity(instanceId, string.Empty)
            {
                Properties =
                {
                    ["CustomStatus"] = new EntityProperty(runtimeState.Status),
                    ["ExecutionId"] = new EntityProperty(executionId),
                    ["LastUpdatedTime"] = new EntityProperty(newEvents.Last().Timestamp),
                }
            };
            
            for (int i = 0; i < newEvents.Count; i++)
            {
                HistoryEvent historyEvent = newEvents[i];
                DynamicTableEntity historyEntity = this.tableEntityConverter.ConvertToTableEntity(historyEvent);
                historyEntity.PartitionKey = instanceId;

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
                        orchestratorEventType = historyEvent.EventType;
                        ExecutionStartedEvent executionStartedEvent = (ExecutionStartedEvent)historyEvent;
                        instanceEntity.Properties["Name"] = new EntityProperty(executionStartedEvent.Name);
                        instanceEntity.Properties["Version"] = new EntityProperty(executionStartedEvent.Version);
                        instanceEntity.Properties["CreatedTime"] = new EntityProperty(executionStartedEvent.Timestamp);
                        instanceEntity.Properties["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.Running.ToString());
                        this.SetInstancesTablePropertyFromHistoryProperty(
                            historyEntity,
                            instanceEntity,
                            historyPropertyName: nameof(executionStartedEvent.Input),
                            instancePropertyName: InputProperty,
                            data: executionStartedEvent.Input);
                        break;
                    case EventType.ExecutionCompleted:
                        orchestratorEventType = historyEvent.EventType;
                        ExecutionCompletedEvent executionCompleted = (ExecutionCompletedEvent)historyEvent;
                        instanceEntity.Properties["RuntimeStatus"] = new EntityProperty(executionCompleted.OrchestrationStatus.ToString());
                        this.SetInstancesTablePropertyFromHistoryProperty(
                            historyEntity,
                            instanceEntity,
                            historyPropertyName: nameof(executionCompleted.Result),
                            instancePropertyName: OutputProperty,
                            data: executionCompleted.Result);
                        break;
                    case EventType.ExecutionTerminated:
                        orchestratorEventType = historyEvent.EventType;
                        ExecutionTerminatedEvent executionTerminatedEvent = (ExecutionTerminatedEvent)historyEvent;
                        instanceEntity.Properties["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.Terminated.ToString());
                        this.SetInstancesTablePropertyFromHistoryProperty(
                            historyEntity,
                            instanceEntity,
                            historyPropertyName: nameof(executionTerminatedEvent.Input),
                            instancePropertyName: OutputProperty,
                            data: executionTerminatedEvent.Input);
                        break;
                    case EventType.ContinueAsNew:
                        orchestratorEventType = historyEvent.EventType;
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
                        executionId,
                        historyEventBatch,
                        newEventListBuffer,
                        newEvents.Count,
                        episodeNumber,
                        estimatedBytes,
                        eTagValue);

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
                    executionId,
                    historyEventBatch,
                    newEventListBuffer,
                    newEvents.Count,
                    episodeNumber,
                    estimatedBytes,
                    eTagValue);
            }

            Stopwatch orchestrationInstanceUpdateStopwatch = Stopwatch.StartNew();
            await this.InstancesTable.ExecuteAsync(TableOperation.InsertOrMerge(instanceEntity));

            this.stats.StorageRequests.Increment();
            this.stats.TableEntitiesWritten.Increment();

            AnalyticsEventSource.Log.InstanceStatusUpdate(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                executionId,
                orchestratorEventType?.ToString() ?? string.Empty,
                episodeNumber,
                orchestrationInstanceUpdateStopwatch.ElapsedMilliseconds,
                Utils.ExtensionVersion);

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
            string instanceId = entity.PartitionKey;
            string sequenceNumber = entity.RowKey;

            string eventType = entity.Properties["EventType"].StringValue;
            string blobName = $"{instanceId}/history-{sequenceNumber}-{eventType}-{property}.json.gz";

            return blobName.ToLowerInvariant();
        }

        async Task<string> UploadHistoryBatch(
            string instanceId,
            string executionId,
            TableBatchOperation historyEventBatch,
            StringBuilder historyEventNamesBuffer,
            int numberOfTotalEvents,
            int episodeNumber,
            int estimatedBatchSizeInBytes,
            string eTagValue)
        {
            // Adding / updating sentinel entity
            DynamicTableEntity sentinelEntity = new DynamicTableEntity(instanceId, SentinelRowKey)
            {
                Properties =
                {
                    ["ExecutionId"] = new EntityProperty(executionId),
                }
            };

            if (!string.IsNullOrEmpty(eTagValue))
            {
                sentinelEntity.ETag = eTagValue;
                historyEventBatch.Replace(sentinelEntity);
            }
            else
            {
                historyEventBatch.InsertOrReplace(sentinelEntity);
            }

            Stopwatch stopwatch = Stopwatch.StartNew();
            IList<TableResult> tableResultList;
            try
            {
                tableResultList = await this.HistoryTable.ExecuteBatchAsync(
                    historyEventBatch,
                    this.StorageTableRequestOptions,
                    null);
            }
            catch (StorageException ex)
            {
                if (ex.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                {
                    AnalyticsEventSource.Log.SplitBrainDetected(
                        this.storageAccountName,
                        this.taskHubName,
                        instanceId,
                        executionId,
                        historyEventBatch.Count,
                        numberOfTotalEvents,
                        historyEventNamesBuffer.ToString(0, historyEventNamesBuffer.Length - 1), // remove trailing comma
                        stopwatch.ElapsedMilliseconds,
                        eTagValue,
                        Utils.ExtensionVersion);
                }

                throw;
            }

            this.stats.StorageRequests.Increment();
            this.stats.TableEntitiesWritten.Increment(historyEventBatch.Count);

            string newETagValue = null;
            if (tableResultList != null)
            {
                for (int i = tableResultList.Count - 1; i >= 0; i--)
                {
                    if (((DynamicTableEntity)tableResultList[i].Result).RowKey == SentinelRowKey)
                    {
                        newETagValue = tableResultList[i].Etag;
                        break;
                    }
                }
            }

            AnalyticsEventSource.Log.AppendedInstanceHistory(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                executionId,
                historyEventBatch.Count,
                numberOfTotalEvents,
                historyEventNamesBuffer.ToString(0, historyEventNamesBuffer.Length - 1), // remove trailing comma
                episodeNumber,
                stopwatch.ElapsedMilliseconds,
                estimatedBatchSizeInBytes,
                string.Concat(eTagValue ?? "(null)", " -->", Environment.NewLine, newETagValue ?? "(null)"),
                Utils.ExtensionVersion);

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

        class HistoryEntitiesResponseInfo
        {
            internal long ElapsedMilliseconds { get; set; }
            internal int RequestCount { get; set; }
            internal List<DynamicTableEntity> HistoryEventEntities { get; set; }
        }
    }
}
