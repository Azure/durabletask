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
    using System.Collections.Concurrent;
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
        readonly CloudTable historyTable;
        readonly CloudTable instancesTable;
        readonly AzureStorageOrchestrationServiceStats stats;
        readonly TableEntityConverter tableEntityConverter;
        readonly IReadOnlyDictionary<EventType, Type> eventTypeMap;
        readonly ConcurrentDictionary<string, string> eTagValues;
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

            this.historyTable = tableClient.GetTableReference(historyTableName);
            this.instancesTable = tableClient.GetTableReference(instancesTableName);

            this.StorageTableRequestOptions = settings.HistoryTableRequestOptions;

            this.eTagValues = new ConcurrentDictionary<string, string>();

            // Use reflection to learn all the different event types supported by DTFx.
            // This could have been hardcoded, but I generally try to avoid hardcoding of point-in-time DTFx knowledge.
            Type historyEventType = typeof(HistoryEvent);

            IEnumerable<Type> historyEventTypes = historyEventType.Assembly.GetTypes().Where(
                t => !t.IsAbstract && t.IsSubclassOf(historyEventType));

            PropertyInfo eventTypeProperty = historyEventType.GetProperty(nameof(HistoryEvent.EventType));
            this.eventTypeMap = historyEventTypes.ToDictionary(
                type => ((HistoryEvent)FormatterServices.GetUninitializedObject(type)).EventType);
        }

        /// <summary>
        ///  Table Request Options for The History and Instance Tables
        /// </summary>
        public TableRequestOptions StorageTableRequestOptions { get; set; }

        internal CloudTable HistoryTable => this.historyTable;

        internal CloudTable InstancesTable => this.instancesTable;

        /// <inheritdoc />
        public override Task CreateAsync()
        {
            return Task.WhenAll(new Task[]
            {
                this.historyTable.CreateIfNotExistsAsync(),
                this.instancesTable.CreateIfNotExistsAsync()
            });
        }

        /// <inheritdoc />
        public override Task DeleteAsync()
        {
            return Task.WhenAll(new Task[]
            {
                this.historyTable.DeleteIfExistsAsync(),
                this.instancesTable.DeleteIfExistsAsync()
            });
        }

        /// <inheritdoc />
        public override async Task<bool> ExistsAsync()
        {
            return this.historyTable != null && this.instancesTable != null && await this.historyTable.ExistsAsync() && await this.instancesTable.ExistsAsync();
        }

        /// <inheritdoc />
        public override async Task<IList<HistoryEvent>> GetHistoryEventsAsync(string instanceId, string expectedExecutionId, CancellationToken cancellationToken = default(CancellationToken))
        {
            var filterCondition = new StringBuilder(200);

            const char Quote = '\'';

            // e.g. "PartitionKey eq 'c138dd969a1e4a699b0644c7d8279f81'"
            filterCondition.Append("PartitionKey eq ").Append(Quote).Append(instanceId).Append(Quote); // = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, instanceId);
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
                var segment = await this.historyTable.ExecuteQuerySegmentedAsync(
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
                        this.eTagValues[instanceId] = entity.ETag;
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
                this.GetETagValue(instanceId),
                Utils.ExtensionVersion);

            return historyEvents;
        }

        /// <inheritdoc />
        public override async Task<IList<OrchestrationState>> GetStateAsync(string instanceId, bool allExecutions)
        {
            return new[] { await this.GetStateAsync(instanceId, executionId: null) };
        }

        /// <inheritdoc />
        public override async Task<OrchestrationState> GetStateAsync(string instanceId, string executionId)
        {
            if (instanceId == null)
            {
                throw new ArgumentNullException(nameof(instanceId));
            }

            var stopwatch = new Stopwatch();
            TableResult orchestration = await this.InstancesTable.ExecuteAsync(TableOperation.Retrieve<OrchestrationInstanceStatus>(instanceId, ""));
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
            return await ConvertFromAsync(orchestrationInstanceStatus, instanceId);
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
        public override async Task<IList<OrchestrationState>> GetStateAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            var query = new TableQuery<OrchestrationInstanceStatus>();
            TableContinuationToken token = null;

            var orchestrationStates = new List<OrchestrationState>(100);

            while (true)
            {
                var segment = await this.instancesTable.ExecuteQuerySegmentedAsync(query, token); // TODO make sure if it has enough parameters

                int previousCount = orchestrationStates.Count;
                var tasks = segment.AsEnumerable<OrchestrationInstanceStatus>().Select(async x => await ConvertFromAsync(x, x.PartitionKey));
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
                    ["LastUpdatedTime"] = new EntityProperty(executionStartedEvent.Timestamp),
                }
            };

            await this.CompressLargeMessageAsync(entity);

            Stopwatch stopwatch = Stopwatch.StartNew();
            await this.instancesTable.ExecuteAsync(
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

        /// <inheritdoc />
        public override Task StartAsync()
        {
            ServicePointManager.FindServicePoint(this.historyTable.Uri).UseNagleAlgorithm = false;
            ServicePointManager.FindServicePoint(this.instancesTable.Uri).UseNagleAlgorithm = false;
            return Utils.CompletedTask;
        }

        /// <inheritdoc />
        public override async Task UpdateStateAsync(OrchestrationRuntimeState runtimeState, string instanceId, string executionId)
        {
            int estimatedBytes = 0;
            IList<HistoryEvent> newEvents = runtimeState.NewEvents;
            IList<HistoryEvent> allEvents = runtimeState.Events;

            var newEventListBuffer = new StringBuilder(4000);
            var historyEventBatch = new TableBatchOperation();

            EventType? orchestratorEventType = null;

            string eTagValue = this.GetETagValue(instanceId);

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

            if (orchestratorEventType == EventType.ExecutionCompleted ||
                orchestratorEventType == EventType.ExecutionFailed ||
                orchestratorEventType == EventType.ExecutionTerminated)
            {
                this.eTagValues.TryRemove(instanceId, out _);
            }
            else
            {
                this.eTagValues[instanceId] = eTagValue;
            }

            Stopwatch orchestrationInstanceUpdateStopwatch = Stopwatch.StartNew();
            await this.instancesTable.ExecuteAsync(TableOperation.InsertOrMerge(orchestrationInstanceUpdate));

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
                tableResultList = await this.historyTable.ExecuteBatchAsync(
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

            if (tableResultList != null)
            {
                for (int i = tableResultList.Count - 1; i >= 0; i--)
                {
                    if (((DynamicTableEntity)tableResultList[i].Result).RowKey == SentinelRowKey)
                    {
                        eTagValue = tableResultList[i].Etag;
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
                this.GetETagValue(instanceId),
                Utils.ExtensionVersion);

            return eTagValue;
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

        string GetETagValue(string instanceId)
        {
            this.eTagValues.TryGetValue(instanceId, out string eTag);
            return eTag;
        }
    }
}
