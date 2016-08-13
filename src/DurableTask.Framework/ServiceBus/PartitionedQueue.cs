//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

using System.Runtime.Serialization;

namespace Microsoft.ServiceBus.Task.ServiceBus
{
    using Microsoft.ServiceBus.Messaging;
    using Microsoft.ServiceBus.Task.History;
    using Microsoft.ServiceBus.Task.Tracing;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Transactions;
    using System.Threading.Tasks;

    public sealed class PartitionedQueue
    {
        readonly static string PartitionSuffix = "/{0}";
        readonly string basePath;
        readonly string connectionString;

        readonly NamespaceManager namespaceManager;

        bool requiresSession;
        int partitionCount;

        MessagingFactory factory;
        QueueClient[] clients;

        object thisLock = new object();

        public PartitionedQueue(string connectionString, string basePath, int partitionCount, bool requiresSession)
        {
            this.requiresSession = requiresSession;
            this.partitionCount = partitionCount;
            this.basePath = basePath;
            this.connectionString = connectionString;
            this.requiresSession = requiresSession;
            this.namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);
            this.factory = MessagingFactory.CreateFromConnectionString(connectionString);
            this.clients = new QueueClient[this.partitionCount];
        }

        string GetQueuePartitionName(int index)
        {
            return this.basePath + string.Format(PartitionSuffix, index);
        }

        // Management operations
        public void Create()
        {
            this.Delete();
            for (int i = 0; i < this.partitionCount; i++)
            {
                CreateQueuePartition(this.GetQueuePartitionName(i));
            }
        }

        public void CreateIfNotExists()
        {
            var queueDescriptions = namespaceManager.GetQueues("startswith(path, '" + this.basePath + "') eq TRUE");

            for (int i = 0; i < this.partitionCount; i++)
            {
                CreateQueuePartitionIfNotExists(queueDescriptions, this.GetQueuePartitionName(i));
            }
        }

        public void Delete()
        {
            for (int i = 0; i < this.partitionCount; i++)
            {
                SafeDeleteQueuePartition(this.GetQueuePartitionName(i));
            }
        }

        void CreateQueuePartition(string path)
        {
            try
            {
                this.namespaceManager.CreateQueue(new QueueDescription(path) {RequiresSession = this.requiresSession});
            }
            catch (MessagingEntityAlreadyExistsException)
            {
                // might have raced with another create call
            }
        }

        void CreateQueuePartitionIfNotExists(IEnumerable<QueueDescription> queueDescriptions, string queuePath)
        {
            if (!queueDescriptions.Where((q) => string.Equals(q.Path, queuePath)).Any())
            {
                CreateQueuePartition(queuePath);
            }
        }

        void SafeDeleteQueuePartition(string path)
        {
            try
            {
                this.namespaceManager.DeleteQueue(path);
            }
            catch (MessagingEntityNotFoundException)
            {
            }
        }

        public long GetActiveMessageCount()
        {
            var queueDescriptions = namespaceManager.GetQueues("startswith(path, '" + this.basePath + "') eq TRUE");
            long count = 0;
            foreach (var queueDescription in queueDescriptions)
            {
                count += queueDescription.MessageCount;
            }
            return count;
        }


        QueueClient GetQueueClient(int index)
        {
            if (this.clients[index] == null)
            {
                lock (this.thisLock)
                {
                    if (this.clients[index] == null)
                    {
                        this.clients[index] = this.factory.CreateQueueClient(this.GetQueuePartitionName(index), ReceiveMode.PeekLock);
                    }
                }
            }
            return this.clients[index];
        }

        // Runtime operations
        public async Task SendBatchAsync(IList<BrokeredMessage> messages)
        {
            if (messages == null || messages.Count == 0)
            {
                return;
            }

            var first = messages[0];
            string sessionId = first.SessionId;
            foreach(var m in messages)
            {
                if (!string.Equals(m.SessionId, sessionId, StringComparison.Ordinal))
                {
                    throw new ArgumentException("All messages in batch must have the same session id");
                }
            }

            QueueClient client = this.GetQueueClient(this.GetPartitionIndex(sessionId));
            await client.SendBatchAsync(messages);
        }

//        public async MessageSession AcceptMessageSession(TimeSpan timeout)
//        {
//            
//        }

        public void Close()
        {
            foreach (var qc in this.clients)
            {
                qc.Close();
            }
            factory.Close();
        }

        // helpers
        int GetPartitionIndex(string partitionKey)
        {
            int partitionIndex = partitionKey.GetHashCode() % this.partitionCount;
            return partitionIndex < 0 ? (partitionIndex*-1) : partitionIndex;
        }
    }
}
