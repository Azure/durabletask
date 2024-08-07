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

namespace DurableTask.AzureStorage
{
    using System;
    using System.Runtime.Serialization;
    using Azure.Storage.Queues.Models;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.Core;
    using Newtonsoft.Json;

    /// <summary>
    /// Protocol class for all Azure Queue messages.
    /// </summary>
    [DataContract]
    public class MessageData
    {
        /// <summary>
        /// The MessageData object.
        /// </summary>
        public MessageData(
            TaskMessage message,
            Guid activityId,
            string queueName,
            int? orchestrationEpisode,
            OrchestrationInstance sender)
        {
            this.TaskMessage = message;
            this.ActivityId = activityId;
            this.QueueName = queueName;
            this.Episode = orchestrationEpisode;
            this.Sender = sender ?? throw new ArgumentNullException(nameof(sender));
        }

        /// <summary>
        /// The MessageData object.
        /// </summary>
        public MessageData()
        { }

        /// <summary>
        /// The Activity ID.
        /// </summary>
        [DataMember]
        public Guid ActivityId { get; private set; }

        /// <summary>
        /// The TaskMessage.
        /// </summary>
        [DataMember]
        public TaskMessage TaskMessage { get; private set; }

        /// <summary>
        /// The blob name for the compressed message. This value is set if there is a compressed blob.
        /// </summary>
        [DataMember]
        public string CompressedBlobName { get; set; }

        /// <summary>
        /// The client-side sequence number of the message.
        /// </summary>
        [DataMember]
        public long SequenceNumber { get; set; }

        /// <summary>
        /// The episode number of the orchestration which created this message.
        /// </summary>
        /// <remarks>
        /// This value may be <c>null</c> if the orchestration instance that created
        /// the message was started before episode numbers were tracked, or if the message
        /// was created by a client.
        /// </remarks>
        [DataMember(EmitDefaultValue = false)]
        public int? Episode { get; private set; }

        /// <summary>
        /// The sender of the message.
        /// </summary>
        [DataMember(EmitDefaultValue = false)]
        [JsonProperty(TypeNameHandling = TypeNameHandling.None)]
        public OrchestrationInstance Sender { get; private set; }

        /// <summary>
        /// TraceContext for correlation.
        /// </summary>
        [DataMember]
        public string SerializableTraceContext { get; set; }

        internal string Id => this.OriginalQueueMessage?.MessageId;

        internal string QueueName { get; set; }

        internal QueueMessage OriginalQueueMessage { get; set; }

        internal long TotalMessageSizeBytes { get; set; }

        internal MessageFormatFlags MessageFormat { get; set; }

        internal void Update(UpdateReceipt receipt)
        {
            this.OriginalQueueMessage = this.OriginalQueueMessage.Update(receipt);
        }
    }

    /// <summary>
    /// The message type.
    /// </summary>
    [Flags]
    public enum MessageFormatFlags
    {
        /// <summary>
        /// Inline JSON message type.
        /// </summary>
        InlineJson = 0b0000,

        /// <summary>
        /// Blob message type.
        /// </summary>
        StorageBlob = 0b0001
    }
}
