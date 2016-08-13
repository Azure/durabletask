//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

using System;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceBus.Task.History;

namespace Microsoft.ServiceBus.Task
{
    public class TaskOrchestrationClient
    {
        readonly MessagingFactory messagingFactory;
        // TODO : change to subscription
        readonly QueueClient oxQueueClient;

        readonly string connectionString;
        readonly string orchestrationTopicName;

        DataConverter defaultConverter;

        public TaskOrchestrationClient(string connectionString, string orchestrationTopicName)
        {
            this.orchestrationTopicName = orchestrationTopicName;
            this.connectionString = connectionString;
            this.messagingFactory = MessagingFactory.CreateFromConnectionString(this.connectionString);
            this.oxQueueClient = this.messagingFactory.CreateQueueClient(this.orchestrationTopicName);
            this.defaultConverter = new JsonDataConverter();
        }

        public string CreateOrchestrationInstance(string name, string version, object input)
        {
            string instanceId = Guid.NewGuid().ToString("N");
            string serializedInput = this.defaultConverter.Serialize(input);
            TaskMessage taskMessage = new TaskMessage();
            taskMessage.InstanceId = instanceId;

            taskMessage.Event = new ExecutionStartedEvent(-1, serializedInput);
            taskMessage.WorkflowDefinition = name;
            taskMessage.Version = version;

            BrokeredMessage brokeredMessage = new BrokeredMessage(taskMessage);
            brokeredMessage.SessionId = taskMessage.InstanceId;
            this.oxQueueClient.Send(brokeredMessage);

            return instanceId;
        }

    }
}
