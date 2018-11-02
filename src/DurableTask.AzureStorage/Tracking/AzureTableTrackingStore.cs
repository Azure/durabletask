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
    /// Tracking store for use with the AzureStorageOrxhestration Service. Uses azure table and blob storage to store runtime state.
    /// </summary>
    class AzureTableTrackingStore : TrackingStoreBase
    {
        const string NameProperty = "Name";
        const string InputProperty = "Input";
        const string ResultProperty = "Result";
        const string OutputProperty = "Output";
        const string BlobNamePropertySuffix = "BlobName";
        const string SentinelRowKey = "sentinel";
        const int MaxStorageQueuePayloadSizeInBytes = 60 * 1024; // 60KB
        const int GuidByteSize = 72;

        static readonly string[] VariableSizeEntityProperties = new[]
        {
            NameProperty,
            InputProperty,
            ResultProperty,
            OutputProperty,
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
            AzureStorageOrchestrationServiceStats stats)
        {
            this.settings = settings;
            this.messageManager = messageManager;
            this.stats = stats;
            this.tableEntityConverter = new TableEntityConverter();
            this.taskHubName = settings.TaskHubName;

            CloudStorageAccount account = CloudStorageAccount.Parse(settings.StorageConnectionString);
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
            var filterCondition = new StringBuilder(200);

            const char Quote = '\'';

            // e.g. "PartitionKey eq 'c138dd969a1e4a699b0644c7d8279f81'"
            filterCondition.Append("PartitionKey eq ").Append(Quote).Append(instanceId).Append(Quote);
            if (expectedExecutionId != null)
            {
                // Filter down to a specific generation.
                // e.g. "PartitionKey eq 'c138dd969a1e4a699b0644c7d8279f81' and ExecutionId eq '85f05ce1494c4a29989f64d3fe0f9089'"
                filterCondition.Append(" and ExecutionId eq ").Append(Quote).Append(expectedExecutionId).Append(Quote);
            }

            TableQuery query = new TableQuery().Where(filterCondition.ToString());

            // TODO: Write-through caching should ensure that we rarely need to make this call?
            var historyEventEntities = new List<DynamicTableEntity>(100);

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

                int previousCount = historyEventEntities.Count;
                historyEventEntities.AddRange(segment);
                this.stats.StorageRequests.Increment();
                this.stats.TableEntitiesRead.Increment(historyEventEntities.Count - previousCount);

                continuationToken = segment.ContinuationToken;
                if (finishedEarly || continuationToken == null || cancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }

            IList<HistoryEvent> historyEvents;
            string executionId;
            string eTagValue = null;
            if (historyEventEntities.Count > 0)
            {
                // The most recent generation will always be in the first history event.
                executionId = historyEventEntities[0].Properties["ExecutionId"].StringValue;

                // Convert the table entities into history events.
                var events = new List<HistoryEvent>(historyEventEntities.Count);

                foreach (DynamicTableEntity entity in historyEventEntities)
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

                    string blobNameKey;

                    if (this.HasCompressedTableEntityByPropertyKey(entity, InputProperty, out blobNameKey))
                    {
                        await this.SetDecompressedTableEntityAsync(entity, InputProperty, blobNameKey);
                    }

                    if (this.HasCompressedTableEntityByPropertyKey(entity, ResultProperty, out blobNameKey))
                    {
                        await this.SetDecompressedTableEntityAsync(entity, ResultProperty, blobNameKey);
                    }

                    if (this.HasCompressedTableEntityByPropertyKey(entity, OutputProperty, out blobNameKey))
                    {
                        await this.SetDecompressedTableEntityAsync(entity, OutputProperty, blobNameKey);
                    }

                    events.Add((HistoryEvent)this.tableEntityConverter.ConvertFromTableEntity(entity, GetTypeForTableEntity));
                }

                historyEvents = events;
            }
            else
            {
                historyEvents = EmptyHistoryEventList;
                executionId = expectedExecutionId;
            }

            AnalyticsEventSource.Log.FetchedInstanceHistory(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                executionId,
                historyEvents.Count,
                requestCount,
                stopwatch.ElapsedMilliseconds,
                eTagValue,
                Utils.ExtensionVersion);

            return new OrchestrationHistory(historyEvents, eTagValue);
        }

        private async Task<List<DynamicTableEntity>> QueryHistoryForRewind(string filterCondition, string instanceId, CancellationToken cancellationToken)
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

            AnalyticsEventSource.Log.FetchedInstanceHistory(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                entities[0].Properties["ExecutionId"].StringValue,
                entities.Count,
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

            orchestratorStartedFilterCondition.Append("PartitionKey eq ").Append(Quote).Append(instanceId).Append(Quote); // = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, instanceId);
            orchestratorStartedFilterCondition.Append(" and EventType eq ").Append(Quote).Append("OrchestratorStarted").Append(Quote);

            var orchestratorStartedEntities = await this.QueryHistoryForRewind(orchestratorStartedFilterCondition.ToString(), instanceId, cancellationToken);

            // get most recent orchestratorStarted event
            var recentStartRowKey = orchestratorStartedEntities.Max(x => x.RowKey);
            var recentStartRow = orchestratorStartedEntities.Where(y => y.RowKey == recentStartRowKey).ToList();
            var executionId = recentStartRow[0].Properties["ExecutionId"].StringValue;
            var instanceTimestamp = recentStartRow[0].Timestamp.DateTime;

            var rowsToUpdateFilterCondition = new StringBuilder(200);

            rowsToUpdateFilterCondition.Append("PartitionKey eq ").Append(Quote).Append(instanceId).Append(Quote);
            rowsToUpdateFilterCondition.Append(" and ExecutionId eq ").Append(Quote).Append(executionId).Append(Quote);
            rowsToUpdateFilterCondition.Append(" and (OrchestrationStatus eq ").Append(Quote).Append("Failed").Append(Quote);
            rowsToUpdateFilterCondition.Append(" or EventType eq").Append(Quote).Append("TaskFailed").Append(Quote);
            rowsToUpdateFilterCondition.Append(" or EventType eq").Append(Quote).Append("SubOrchestrationInstanceFailed").Append(Quote).Append(")");

            var entitiesToClear = await this.QueryHistoryForRewind(rowsToUpdateFilterCondition.ToString(), instanceId, cancellationToken);

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

                    tsFilterCondition.Append("PartitionKey eq ").Append(Quote).Append(instanceId).Append(Quote);
                    tsFilterCondition.Append(" and ExecutionId eq ").Append(Quote).Append(executionId).Append(Quote);
                    tsFilterCondition.Append(" and EventId eq ").Append(taskScheduledId);
                    tsFilterCondition.Append(" and EventType eq ").Append(Quote).Append(nameof(EventType.TaskScheduled)).Append(Quote);

                    var taskScheduledEntities = await QueryHistoryForRewind(tsFilterCondition.ToString(), instanceId, cancellationToken);

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

                    soFilterCondition.Append("PartitionKey eq ").Append(Quote).Append(instanceId).Append(Quote);
                    soFilterCondition.Append(" and ExecutionId eq ").Append(Quote).Append(executionId).Append(Quote);
                    soFilterCondition.Append(" and EventId eq ").Append(subOrchestrationId);
                    soFilterCondition.Append(" and EventType eq ").Append(Quote).Append(nameof(EventType.SubOrchestrationInstanceCreated)).Append(Quote);

                    var subOrchesratrationEntities = await QueryHistoryForRewind(soFilterCondition.ToString(), instanceId, cancellationToken);

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

            return await this.ConvertFromAsync(orchestrationInstanceStatus, instanceId);
        }

        private async Task<OrchestrationState> ConvertFromAsync(OrchestrationInstanceStatus orchestrationInstanceStatus, string instanceId)
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

            string[] results = await Task.WhenAll(
                this.GetOrchestrationInputAsync(orchestrationInstanceStatus),
                this.GetOrchestrationOutputAsync(orchestrationInstanceStatus));
            orchestrationState.Input = results[0];
            orchestrationState.Output = results[1];

            return orchestrationState;
        }

        /// <inheritdoc />
        public override Task<IList<OrchestrationState>> GetStateAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            var query = new TableQuery<OrchestrationInstanceStatus>();
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

        private async Task<DurableStatusQueryResult> QueryStateAsync(TableQuery<OrchestrationInstanceStatus> query, int top, string continuationToken, CancellationToken cancellationToken)
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
            var tasks = segment.Select(status => this.ConvertFromAsync(status, status.PartitionKey));
            OrchestrationState[] result = await Task.WhenAll(tasks);
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

        private async Task<IList<OrchestrationState>> QueryStateAsync(TableQuery<OrchestrationInstanceStatus> query, CancellationToken cancellationToken)
        {
            TableContinuationToken token = null;
            var orchestrationStates = new List<OrchestrationState>(100);
            while (true)
            {
                var segment = await this.InstancesTable.ExecuteQuerySegmentedAsync(query, token);

                int previousCount = orchestrationStates.Count;
                var tasks = segment.Select(async status => await this.ConvertFromAsync(status, status.PartitionKey));
                OrchestrationState[] result = await Task.WhenAll(tasks);
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

        /// <inheritdoc />
        public override Task PurgeHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public override async Task SetNewExecutionAsync(ExecutionStartedEvent executionStartedEvent)
        {
            DynamicTableEntity entity = new DynamicTableEntity(executionStartedEvent.OrchestrationInstance.InstanceId, "")
            {
                Properties =
                {
                    ["Input"] = new EntityProperty(executionStartedEvent.Input),
                    ["CreatedTime"] = new EntityProperty(executionStartedEvent.Timestamp),
                    ["Name"] = new EntityProperty(executionStartedEvent.Name),
                    ["Version"] = new EntityProperty(executionStartedEvent.Version),
                    ["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.Pending.ToString()),
                    ["LastUpdatedTime"] = new EntityProperty(DateTime.UtcNow),
                }
            };

            await this.CompressLargeMessageAsync(entity);

            Stopwatch stopwatch = Stopwatch.StartNew();
            await this.InstancesTable.ExecuteAsync(
                TableOperation.InsertOrReplace(entity));
            this.stats.StorageRequests.Increment();
            this.stats.TableEntitiesWritten.Increment(1);

            AnalyticsEventSource.Log.InstanceStatusUpdate(
                this.storageAccountName,
                this.taskHubName,
                executionStartedEvent.OrchestrationInstance.InstanceId,
                executionStartedEvent.OrchestrationInstance.ExecutionId,
                executionStartedEvent.EventType.ToString(),
                stopwatch.ElapsedMilliseconds,
                Utils.ExtensionVersion);
        }


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

            await this.CompressLargeMessageAsync(entity);

            Stopwatch stopwatch = Stopwatch.StartNew();
            await this.InstancesTable.ExecuteAsync(TableOperation.Merge(entity));
            this.stats.StorageRequests.Increment();
            this.stats.TableEntitiesWritten.Increment(1);

            AnalyticsEventSource.Log.InstanceStatusUpdate(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                string.Empty,
                nameof(EventType.GenericEvent),
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

            var newEventListBuffer = new StringBuilder(4000);
            var historyEventBatch = new TableBatchOperation();

            EventType? orchestratorEventType = null;

            DynamicTableEntity orchestrationInstanceUpdate = new DynamicTableEntity(instanceId, "")
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
                DynamicTableEntity entity = this.tableEntityConverter.ConvertToTableEntity(historyEvent);

                await this.CompressLargeMessageAsync(entity);

                newEventListBuffer.Append(historyEvent.EventType.ToString()).Append(',');

                // The row key is the sequence number, which represents the chronological ordinal of the event.
                long sequenceNumber = i + (allEvents.Count - newEvents.Count);
                entity.RowKey = sequenceNumber.ToString("X16");
                entity.PartitionKey = instanceId;
                entity.Properties["ExecutionId"] = new EntityProperty(executionId);

                // Replacement can happen if the orchestration episode gets replayed due to a commit failure in one of the steps below.
                historyEventBatch.InsertOrReplace(entity);

                // Keep track of the byte count to ensure we don't hit the 4 MB per-batch maximum
                estimatedBytes += GetEstimatedByteCount(entity);

                // Monitor for orchestration instance events 
                switch (historyEvent.EventType)
                {
                    case EventType.ExecutionStarted:
                        orchestratorEventType = historyEvent.EventType;
                        ExecutionStartedEvent executionStartedEvent = (ExecutionStartedEvent)historyEvent;
                        orchestrationInstanceUpdate.Properties["Name"] = new EntityProperty(executionStartedEvent.Name);
                        orchestrationInstanceUpdate.Properties["Version"] = new EntityProperty(executionStartedEvent.Version);
                        orchestrationInstanceUpdate.Properties["CreatedTime"] = new EntityProperty(executionStartedEvent.Timestamp);
                        orchestrationInstanceUpdate.Properties["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.Running.ToString());
                        this.SetTablePropertyForMessage(entity, orchestrationInstanceUpdate, InputProperty, InputProperty, executionStartedEvent.Input);
                        break;
                    case EventType.ExecutionCompleted:
                        orchestratorEventType = historyEvent.EventType;
                        ExecutionCompletedEvent executionCompleted = (ExecutionCompletedEvent)historyEvent;
                        this.SetTablePropertyForMessage(entity, orchestrationInstanceUpdate, ResultProperty, OutputProperty, executionCompleted.Result);
                        orchestrationInstanceUpdate.Properties["RuntimeStatus"] = new EntityProperty(executionCompleted.OrchestrationStatus.ToString());
                        break;
                    case EventType.ExecutionTerminated:
                        orchestratorEventType = historyEvent.EventType;
                        ExecutionTerminatedEvent executionTerminatedEvent = (ExecutionTerminatedEvent)historyEvent;
                        this.SetTablePropertyForMessage(entity, orchestrationInstanceUpdate, InputProperty, OutputProperty, executionTerminatedEvent.Input);
                        orchestrationInstanceUpdate.Properties["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.Terminated.ToString());
                        break;
                    case EventType.ContinueAsNew:
                        orchestratorEventType = historyEvent.EventType;
                        ExecutionCompletedEvent executionCompletedEvent = (ExecutionCompletedEvent)historyEvent;
                        this.SetTablePropertyForMessage(entity, orchestrationInstanceUpdate, ResultProperty, OutputProperty, executionCompletedEvent.Result);
                        orchestrationInstanceUpdate.Properties["RuntimeStatus"] = new EntityProperty(OrchestrationStatus.ContinuedAsNew.ToString());
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
                    estimatedBytes,
                    eTagValue);
            }

            Stopwatch orchestrationInstanceUpdateStopwatch = Stopwatch.StartNew();
            await this.InstancesTable.ExecuteAsync(TableOperation.InsertOrMerge(orchestrationInstanceUpdate));

            this.stats.StorageRequests.Increment();
            this.stats.TableEntitiesWritten.Increment();

            AnalyticsEventSource.Log.InstanceStatusUpdate(
                this.storageAccountName,
                this.taskHubName,
                instanceId,
                executionId,
                orchestratorEventType?.ToString() ?? string.Empty,
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

        void SetPropertyMessageToEmptyString(DynamicTableEntity entity)
        {
            if (entity.Properties.ContainsKey(InputProperty) && entity.Properties[InputProperty] != null)
            {
                entity.Properties[InputProperty].StringValue = string.Empty;
            }
            else if (entity.Properties.ContainsKey(ResultProperty) && entity.Properties[ResultProperty] != null)
            {
                entity.Properties[ResultProperty].StringValue = string.Empty;
            }
            else if (entity.Properties.ContainsKey(OutputProperty) && entity.Properties[OutputProperty] != null)
            {
                entity.Properties[OutputProperty].StringValue = string.Empty;
            }
        }

        byte[] GetPropertyMessageAsBytes(DynamicTableEntity entity)
        {
            byte[] messageBytes = new byte[0];

            if (entity.Properties.ContainsKey(InputProperty) && entity.Properties[InputProperty] != null)
            {
                messageBytes = Encoding.UTF8.GetBytes(entity.Properties[InputProperty].StringValue);
            }
            else if (entity.Properties.ContainsKey(ResultProperty) && entity.Properties[ResultProperty] != null)
            {
                messageBytes = Encoding.UTF8.GetBytes(entity.Properties[ResultProperty].StringValue);
            }
            else if (entity.Properties.ContainsKey(OutputProperty) && entity.Properties[OutputProperty] != null)
            {
                messageBytes = Encoding.UTF8.GetBytes(entity.Properties[OutputProperty].StringValue);
            }

            return messageBytes;
        }

        string GetLargeTableEntity(DynamicTableEntity entity)
        {
            if (this.GetLargeTableEntityInternal(entity, InputProperty) != null)
            {
                return InputProperty;
            }

            if (this.GetLargeTableEntityInternal(entity, ResultProperty) != null)
            {
                return ResultProperty;
            }

            if (this.GetLargeTableEntityInternal(entity, OutputProperty) != null)
            {
                return OutputProperty;
            }

            return null;
        }

        string GetLargeTableEntityInternal(DynamicTableEntity entity, string propertyKey)
        {
            if (entity.Properties.TryGetValue(propertyKey, out EntityProperty value)
                && this.ExceedsMaxTableEntitySize(value?.StringValue))
            {
                return propertyKey;
            }

            return null;
        }

        // Assigns the target table entity property. Any large message for type 'Input, or 'Output' would have been compressed earlier as part of the 'entity' object,
        // so, we only need to assign the 'entity' object's blobName to the target table entity blob name property.
        void SetTablePropertyForMessage(DynamicTableEntity entity, DynamicTableEntity orchestrationInstanceUpdate, string sourcePropertyKey, string targetPropertyKey, string message)
        {
            string blobNameKey;

            // Check if the source property has a compressed blob and swap the source with the target property
            if (this.HasCompressedTableEntityByPropertyKey(entity, sourcePropertyKey, out blobNameKey))
            {
                string blobName = entity.Properties[blobNameKey].StringValue;
                orchestrationInstanceUpdate.Properties[sourcePropertyKey] = new EntityProperty(string.Empty);
                string targetBlobNameKey = $"{targetPropertyKey}{BlobNamePropertySuffix}";

                orchestrationInstanceUpdate.Properties[targetBlobNameKey] = new EntityProperty(blobName);
            }
            else
            {
                orchestrationInstanceUpdate.Properties[targetPropertyKey] = new EntityProperty(message);
            }
        }

        async Task CompressLargeMessageAsync(DynamicTableEntity entity)
        {
            string propertyKey = this.GetLargeTableEntity(entity);
            if (propertyKey != null)
            {
                string blobName = Guid.NewGuid().ToString();

                // e.g.InputBlobName, OutputBlobName, ResultBlobName
                string blobNameKey = $"{propertyKey}{BlobNamePropertySuffix}";
                byte[] messageBytes = this.GetPropertyMessageAsBytes(entity);
                await this.messageManager.CompressAndUploadAsBytesAsync(messageBytes, blobName);
                entity.Properties.Add(blobNameKey, new EntityProperty(blobName));
                this.SetPropertyMessageToEmptyString(entity);
            }
        }

        async Task<string> UploadHistoryBatch(
            string instanceId,
            string executionId,
            TableBatchOperation historyEventBatch,
            StringBuilder historyEventNamesBuffer,
            int numberOfTotalEvents,
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
                stopwatch.ElapsedMilliseconds,
                estimatedBatchSizeInBytes,
                string.Concat(eTagValue ?? "(null)", " -->", Environment.NewLine, newETagValue ?? "(null)"),
                Utils.ExtensionVersion);

            return newETagValue;
        }

        async Task<string> GetOrchestrationOutputAsync(OrchestrationInstanceStatus orchestrationInstanceStatus)
        {
            if (string.IsNullOrEmpty(orchestrationInstanceStatus.OutputBlobName))
            {
                return orchestrationInstanceStatus.Output;
            }

            return await this.messageManager.DownloadAndDecompressAsBytesAsync(orchestrationInstanceStatus.OutputBlobName);
        }

        async Task<string> GetOrchestrationInputAsync(OrchestrationInstanceStatus orchestrationInstanceStatus)
        {
            if (string.IsNullOrEmpty(orchestrationInstanceStatus.InputBlobName))
            {
                return orchestrationInstanceStatus.Input;
            }

            return await this.messageManager.DownloadAndDecompressAsBytesAsync(orchestrationInstanceStatus.InputBlobName);
        }

        async Task SetDecompressedTableEntityAsync(DynamicTableEntity dynamicTableEntity, string propertyKey, string blobNameKey)
        {
            string blobName = dynamicTableEntity.Properties[blobNameKey].StringValue;
            string decompressedMessage = await this.messageManager.DownloadAndDecompressAsBytesAsync(blobName);
            dynamicTableEntity.Properties[propertyKey] = new EntityProperty(decompressedMessage);
        }

        // Checks if the table entity has a compressed 'Input', 'Output', or 'Result' blob
        bool HasCompressedTableEntityByPropertyKey(DynamicTableEntity dynamicTableEntity, string propertyKey, out string blobNameKey)
        {
            // e.g. InputBlobName, OutputBlobName, ResultBlobName
            blobNameKey = $"{propertyKey}{BlobNamePropertySuffix}";
            return dynamicTableEntity.Properties.ContainsKey(propertyKey)
                && dynamicTableEntity.Properties.ContainsKey(blobNameKey)
                && dynamicTableEntity.Properties[propertyKey].StringValue == string.Empty;
        }

        bool ExceedsMaxTableEntitySize(string data)
        {
            if (!string.IsNullOrEmpty(data) && Encoding.Unicode.GetByteCount(data) > MaxStorageQueuePayloadSizeInBytes)
            {
                return true;
            }

            return false;
        }
    }
}
