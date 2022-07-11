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
using DurableTask;

namespace DurableTask.AzureStorage.Entities
{
    using System;
    using System.Text;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Data.Tables;

    sealed class HistoryEntity : ITableEntity
    {
        // See https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-the-table-service-data-model#property-types
        const int MaxTablePropertySizeInBytes = 60 * 1024; // 60KB to give buffer
        const string SentinelRowKey = "sentinel";

        public DateTime? CheckpointCompletedTimestamp { get; set; }

        public string? Correlation { get; set; }

        public string? Details { get; set; }

        public ETag ETag { get; set; }

        public int? EventId { get; set; }

        public string? EventType { get; set; }

        public string? ExecutionId { get; set; }

        public string? FailureDetails { get; set; }

        public string? Input { get; set; }

        public string? InstanceId { get; set; }

        internal bool IsSentinelRow => RowKey == SentinelRowKey;

        public string? Name { get; set; }

        public string? OrchestrationStatus { get; set; }

        public string? Output { get; set; }

        public string? PartitionKey { get; set; }

        public string? Reason { get; set; }

        public string? Result { get; set; }

        public string? RowKey { get; set; }

        public int? TaskScheduledId { get; set; }

        public DateTimeOffset? Timestamp { get; set; }

        #region Compression Properties

        public string? CorrelationBlobName { get; set; }

        public string? DetailsBlobName { get; set; }

        public string? FailureDetailsBlobName { get; set; }

        public string? InputBlobName { get; set; }

        public string? NameBlobName { get; set; }

        public string? OutputBlobName { get; set; }

        public string? ReasonBlobName { get; set; }

        public string? ResultBlobName { get; set; }

        #endregion

        #region Compression Methods

        public async Task CompressAsync(MessageManager messageManager)
        {
            if (messageManager == null)
            {
                throw new ArgumentNullException(nameof(messageManager));
            }

            // Compress each of the variable-length columns.
            // Clear out the original property value and create a new "*BlobName"-suffixed property.
            // The runtime will look for the new "*BlobName"-suffixed column to know if a property is stored in a blob.
            string? blobName;

            blobName = await this.CompressPropertyAsync(messageManager, nameof(this.Correlation), this.Correlation);
            if (blobName != null)
            {
                this.CorrelationBlobName = blobName;
                this.Correlation = null;
            }

            blobName = await this.CompressPropertyAsync(messageManager, nameof(this.Details), this.Details);
            if (blobName != null)
            {
                this.DetailsBlobName = blobName;
                this.Details = null;
            }

            blobName = await this.CompressPropertyAsync(messageManager, nameof(this.FailureDetails), this.FailureDetails);
            if (blobName != null)
            {
                this.FailureDetailsBlobName = blobName;
                this.FailureDetails = null;
            }

            blobName = await this.CompressPropertyAsync(messageManager, nameof(this.Input), this.Input);
            if (blobName != null)
            {
                this.InputBlobName = blobName;
                this.Input = null;
            }

            blobName = await this.CompressPropertyAsync(messageManager, nameof(this.Name), this.Name);
            if (blobName != null)
            {
                this.NameBlobName = blobName;
                this.Name = null;
            }

            blobName = await this.CompressPropertyAsync(messageManager, nameof(this.Output), this.Output);
            if (blobName != null)
            {
                this.OutputBlobName = blobName;
                this.Output = null;
            }

            blobName = await this.CompressPropertyAsync(messageManager, nameof(this.Reason), this.Reason);
            if (blobName != null)
            {
                this.ReasonBlobName = blobName;
                this.Reason = null;
            }

            blobName = await this.CompressPropertyAsync(messageManager, nameof(this.Result), this.Result);
            if (blobName != null)
            {
                this.ResultBlobName = blobName;
                this.Result = null;
            }
        }

        public async Task DecompressAsync(MessageManager messageManager)
        {
            if (messageManager == null)
            {
                throw new ArgumentNullException(nameof(messageManager));
            }

            if (this.CorrelationBlobName != null)
            {
                this.Correlation = await this.DecompressPropertyAsync(messageManager, this.CorrelationBlobName);
                this.CorrelationBlobName = null;
            }

            if (this.DetailsBlobName != null)
            {
                this.Details = await this.DecompressPropertyAsync(messageManager, this.DetailsBlobName);
                this.DetailsBlobName = null;
            }

            if (this.FailureDetailsBlobName != null)
            {
                this.FailureDetails = await this.DecompressPropertyAsync(messageManager, this.FailureDetailsBlobName);
                this.FailureDetailsBlobName = null;
            }

            if (this.InputBlobName != null)
            {
                this.Input = await this.DecompressPropertyAsync(messageManager, this.InputBlobName);
                this.InputBlobName = null;
            }

            if (this.NameBlobName != null)
            {
                this.Name = await this.DecompressPropertyAsync(messageManager, this.NameBlobName);
                this.NameBlobName = null;
            }

            if (this.OutputBlobName != null)
            {
                this.Output = await this.DecompressPropertyAsync(messageManager, this.OutputBlobName);
                this.OutputBlobName = null;
            }

            if (this.ReasonBlobName != null)
            {
                this.Reason = await this.DecompressPropertyAsync(messageManager, this.ReasonBlobName);
                this.ReasonBlobName = null;
            }

            if (this.ResultBlobName != null)
            {
                this.Result = await this.DecompressPropertyAsync(messageManager, this.ResultBlobName);
                this.ResultBlobName = null;
            }
        }

        async Task<string?> CompressPropertyAsync(MessageManager messageManager, string property, string? value)
        {
            if (!ExceedsMaxTablePropertySize(value))
            {
                return null;
            }

            // Upload the large property as a blob in Blob Storage since it won't fit in table storage.
            string blobName = this.GetBlobName(property);
            byte[] messageBytes = Encoding.UTF8.GetBytes(value);
            await messageManager.CompressAndUploadAsBytesAsync(messageBytes, blobName);

            return blobName;
        }

        Task<string> DecompressPropertyAsync(MessageManager messageManager, string blobName)
        {
            return messageManager.DownloadAndDecompressAsBytesAsync(blobName);
        }

        string GetBlobName(string property)
        {
            string eventType;
            if (this.EventType != null)
            {
                eventType = this.EventType.GetValueOrDefault().ToString("G");
            }
            else if (property == nameof(this.Input))
            {
                // This message is just to start the orchestration, so it does not have a corresponding
                // EventType. Use a hardcoded value to record the orchestration input.
                eventType = "Input";
            }
            else
            {
                throw new InvalidOperationException($"Could not compute the blob name for property {property}");
            }

            return $"{this.PartitionKey}/history-{this.RowKey}-{eventType}-{property}.json.gz";
        }

        static bool ExceedsMaxTablePropertySize(string? data)
        {
            return !string.IsNullOrEmpty(data) && Encoding.Unicode.GetByteCount(data) > MaxTablePropertySizeInBytes;
        }

        #endregion
    }
}
