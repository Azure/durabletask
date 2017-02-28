//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace DurableTask.AzureStorage
{
    using System;
    using System.Runtime.Serialization;
    using DurableTask;
    using Microsoft.WindowsAzure.Storage.Queue;

    /// <summary>
    /// Protocol class for all Azure Queue messages.
    /// </summary>
    [DataContract]
    class MessageData
    {
        public MessageData(TaskMessage message, Guid activityId)
        {
            this.TaskMessage = message;
            this.ActivityId = activityId;
        }

        public MessageData()
        { }

        [DataMember]
        public Guid ActivityId { get; private set; }

        [DataMember]
        public TaskMessage TaskMessage { get; private set; }

        internal CloudQueueMessage OriginalQueueMessage { get; set; }

        internal long TotalMessageSizeBytes { get; set; }
    }
}
