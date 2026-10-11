#pragma warning disable 1591
namespace DurableTask.ServiceBus.Common.Abstraction
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    /// <inheritdoc />
    public class IMessageSession
    {
        Azure.Messaging.ServiceBus.ServiceBusSessionReceiver sessionReceiver;

        public IMessageSession(Azure.Messaging.ServiceBus.ServiceBusSessionReceiver sessionReceiver)
        {
            this.sessionReceiver = sessionReceiver;
        }

        public string SessionId => this.sessionReceiver.SessionId;

        public DateTime LockedUntilUtc => this.sessionReceiver.SessionLockedUntil.UtcDateTime;

        public async Task<byte[]> GetStateAsync()
        {
            return (await this.sessionReceiver.GetSessionStateAsync())?.ToArray();
        }

        public async Task SetStateAsync(byte[] sessionState)
        {
            if (sessionState == null)
            {
                // Setting session state to null is equivalent to deleting session state.
                // Setting session state to empty byte array is not equivalent to deleting session state.
                await this.sessionReceiver.SetSessionStateAsync(null);
            }
            else
            {
                await this.sessionReceiver.SetSessionStateAsync(new BinaryData(sessionState));
            }
        }

        public async Task RenewSessionLockAsync()
        {
            await this.sessionReceiver.RenewSessionLockAsync();
        }

        public async Task AbandonAsync(Message message)
        {
            await this.sessionReceiver.AbandonMessageAsync(message);
        }

        public async Task<IList<Message>> ReceiveAsync(int maxMessageCount)
        {
            return (await this.sessionReceiver.ReceiveMessagesAsync(maxMessageCount)).Select(x => (Message)x).ToList();
        }

        public async Task CompleteAsync(IEnumerable<Message> messages)
        {
            foreach (var message in messages)
            {
                await this.sessionReceiver.CompleteMessageAsync(message);
            }
        }

        public async Task CloseAsync()
        {
            await this.sessionReceiver.CloseAsync();
        }

    }

    public abstract class Union<A, B>
    {
        public abstract T Match<T>(Func<A, T> f, Func<B, T> g);

        public abstract void Switch(Action<A> f, Action<B> g);

        private Union() { }

        public sealed class Case1 : Union<A, B>
        {
            public readonly A Item;
            public Case1(A item) : base() { this.Item = item; }
            public override T Match<T>(Func<A, T> f, Func<B, T> g)
            {
                return f(Item);
            }
            public override void Switch(Action<A> f, Action<B> g)
            {
                f(Item);
            }
        }

        public sealed class Case2 : Union<A, B>
        {
            public readonly B Item;
            public Case2(B item) { this.Item = item; }
            public override T Match<T>(Func<A, T> f, Func<B, T> g)
            {
                return g(Item);
            }
            public override void Switch(Action<A> f, Action<B> g)
            {
                g(Item);
            }
        }
    }

    /// <inheritdoc />
    public class Message
    {
        private readonly Union<Azure.Messaging.ServiceBus.ServiceBusMessage, Azure.Messaging.ServiceBus.ServiceBusReceivedMessage> message;

        public Message(Azure.Messaging.ServiceBus.ServiceBusMessage msg)
        {
            this.message = new Union<Azure.Messaging.ServiceBus.ServiceBusMessage, Azure.Messaging.ServiceBus.ServiceBusReceivedMessage>.Case1(msg);
            this.SystemProperties = new SystemPropertiesCollection(message);
        }

        public Message(Azure.Messaging.ServiceBus.ServiceBusReceivedMessage msg)
        {
            this.message = new Union<Azure.Messaging.ServiceBus.ServiceBusMessage, Azure.Messaging.ServiceBus.ServiceBusReceivedMessage>.Case2(msg);
            this.SystemProperties = new SystemPropertiesCollection(message);
        }

        public static implicit operator Message(Azure.Messaging.ServiceBus.ServiceBusMessage m)
        {
            return m == null ? null : new Message(m);
        }

        public static implicit operator Azure.Messaging.ServiceBus.ServiceBusMessage(Message m)
        {
            return m.message.Match(
                m => m,
                rm => new Azure.Messaging.ServiceBus.ServiceBusMessage(rm));
        }

        public static implicit operator Message(Azure.Messaging.ServiceBus.ServiceBusReceivedMessage m)
        {
            return m == null ? null : new Message(m);
        }

        public static implicit operator Azure.Messaging.ServiceBus.ServiceBusReceivedMessage(Message m)
        {
            return m.message.Match(
                m => throw NotReceivedMessagePropertyGetException(),
                rm => rm);
        }

        public Message()
        {
            this.message = new Union<Azure.Messaging.ServiceBus.ServiceBusMessage, Azure.Messaging.ServiceBus.ServiceBusReceivedMessage>.Case1(new Azure.Messaging.ServiceBus.ServiceBusMessage());
            this.SystemProperties = new SystemPropertiesCollection(message);
        }

        public Message(byte[] serializableObject)
        {
            this.message = new Union<Azure.Messaging.ServiceBus.ServiceBusMessage, Azure.Messaging.ServiceBus.ServiceBusReceivedMessage>.Case1(new Azure.Messaging.ServiceBus.ServiceBusMessage(new BinaryData(serializableObject)));
            this.SystemProperties = new SystemPropertiesCollection(message);
        }

        public string MessageId
        {
            get => this.message?.Match(
                m => m.MessageId,
                rm => rm.MessageId);
            set => this.message.Switch(
                m => { m.MessageId = value; },
                rm => throw ReceivedMessagePropertySetException());
        }

        public DateTime ScheduledEnqueueTimeUtc
        {
            get => this.message.Match(
                m => m.ScheduledEnqueueTime.UtcDateTime,
                rm => rm.ScheduledEnqueueTime.UtcDateTime);
            set => this.message.Switch(
                m => { m.ScheduledEnqueueTime = new DateTimeOffset(value); },
                rm => throw ReceivedMessagePropertySetException());
        }

        public SystemPropertiesCollection SystemProperties { get; private set; }

        public IDictionary<string, object> UserProperties => this.message.Match(
            m => m.ApplicationProperties,
            rm => (IDictionary<string, object>)rm.ApplicationProperties);

        public byte[] Body => this.message.Match(
            m => m.Body.ToArray(),
            rm => rm.Body.ToArray());

        public string SessionId
        {
            get => this.message?.Match(
                m => m.SessionId,
                rm => rm.SessionId);
            set => this.message.Switch(
                m => { m.SessionId = value; },
                rm => throw ReceivedMessagePropertySetException());
        }

        private static Exception NotReceivedMessagePropertyGetException()
        {
            return new InvalidOperationException("The property cannot be accessed because the message is not received.");
        }

        private static Exception ReceivedMessagePropertySetException()
        {
            return new InvalidOperationException("The property cannot be set because the message is received and the value is readonly.");
        }

        public class SystemPropertiesCollection
        {
            private readonly Union<Azure.Messaging.ServiceBus.ServiceBusMessage, Azure.Messaging.ServiceBus.ServiceBusReceivedMessage> msg;

            public SystemPropertiesCollection(Union<Azure.Messaging.ServiceBus.ServiceBusMessage, Azure.Messaging.ServiceBus.ServiceBusReceivedMessage> msg)
            {
                this.msg = msg;
            }

            public Guid LockToken => this.msg.Match(
                _ => throw NotReceivedMessagePropertyGetException(),
                rm => Guid.Parse(rm.LockToken));

            public int DeliveryCount => this.msg.Match(
                _ => throw NotReceivedMessagePropertyGetException(),
                rm => rm.DeliveryCount);

            public DateTime LockedUntilUtc => this.msg.Match(
                _ => throw NotReceivedMessagePropertyGetException(),
                rm => rm.LockedUntil.UtcDateTime);

            public long SequenceNumber => this.msg.Match(
                _ => throw NotReceivedMessagePropertyGetException(),
                rm => rm.SequenceNumber);

            public DateTime EnqueuedTimeUtc => this.msg.Match(
                _ => throw NotReceivedMessagePropertyGetException(),
                rm => rm.EnqueuedTime.UtcDateTime);
        }

    }

    /// <inheritdoc />
    public abstract class RetryPolicy : Azure.Messaging.ServiceBus.ServiceBusRetryPolicy
    {
    }

    /// <inheritdoc />
    public class ServiceBusConnection
    {
        private readonly Dictionary<string, Azure.Messaging.ServiceBus.ServiceBusClient> sendViaEntityScopedServiceBusClients;
        private readonly Func<Azure.Messaging.ServiceBus.ServiceBusClient> serviceBusClientFactory;

        public ServiceBusConnection(ServiceBusConnectionStringBuilder connectionStringBuilder)
        {
            var options = new Azure.Messaging.ServiceBus.ServiceBusClientOptions
            {
                EnableCrossEntityTransactions = true,
            };

            sendViaEntityScopedServiceBusClients = new Dictionary<string, Azure.Messaging.ServiceBus.ServiceBusClient>();
            serviceBusClientFactory = () => new Azure.Messaging.ServiceBus.ServiceBusClient(
                connectionStringBuilder.ConnectionString,
                options);
        }

        public ServiceBusConnection(string endpoint, Azure.Messaging.ServiceBus.ServiceBusTransportType transportType, Azure.Core.TokenCredential tokenCredential, RetryPolicy retryPolicy = null)
        {
            var options = new Azure.Messaging.ServiceBus.ServiceBusClientOptions
            {
                EnableCrossEntityTransactions = true,
            };

            sendViaEntityScopedServiceBusClients = new Dictionary<string, Azure.Messaging.ServiceBus.ServiceBusClient>();
            serviceBusClientFactory = () => new Azure.Messaging.ServiceBus.ServiceBusClient(
                endpoint,
                tokenCredential,
                options);
        }

        public Azure.Messaging.ServiceBus.ServiceBusClient GetSendViaEntityScopedClient(string sendViaEntity)
        {
            if (sendViaEntityScopedServiceBusClients.TryGetValue(sendViaEntity, out var client))
            {
                return client;
            }

            var newClient = serviceBusClientFactory();
            sendViaEntityScopedServiceBusClients.Add(sendViaEntity, newClient);
            return newClient;
        }
    }

    /// <inheritdoc />
    public class ServiceBusConnectionStringBuilder
    {
        public string ConnectionString { get; set; }

        public Azure.Messaging.ServiceBus.ServiceBusConnectionStringProperties ConnectionStringProperties { get; set; }

        public ServiceBusConnectionStringBuilder(string connectionString)
        {
            ConnectionString = connectionString;
            ConnectionStringProperties = Azure.Messaging.ServiceBus.ServiceBusConnectionStringProperties.Parse(ConnectionString);
        }

        public string SasKeyName => ConnectionStringProperties.SharedAccessKeyName;

        public string SasKey => ConnectionStringProperties.SharedAccessKey;
    }

    /// <inheritdoc />
    public class MessageSender
    {
        private readonly Azure.Messaging.ServiceBus.ServiceBusSender sender;

        public MessageSender(ServiceBusConnection serviceBusConnection, string entityPath, RetryPolicy retryPolicy = null)
        {
            sender = serviceBusConnection.GetSendViaEntityScopedClient(entityPath).CreateSender(entityPath);
        }

        public MessageSender(ServiceBusConnection serviceBusConnection, string entityPath, string viaEntityPath, RetryPolicy retryPolicy = null)
        {
            sender = serviceBusConnection.GetSendViaEntityScopedClient(viaEntityPath).CreateSender(entityPath);
        }

        public async Task SendAsync(Message message)
        {
            await sender.SendMessageAsync(message);
        }

        public async Task SendAsync(IEnumerable<Message> messageList)
        {
            var batch = await sender.CreateMessageBatchAsync();
            foreach (var message in messageList)
            {
                batch.TryAddMessage(message);
            }

            await sender.SendMessagesAsync(batch);
        }

        public async Task CloseAsync()
        {
            await sender.CloseAsync();
        }

    }

    /// <inheritdoc />
    public class MessageReceiver
    {
        private readonly Azure.Messaging.ServiceBus.ServiceBusReceiver receiver;

        public MessageReceiver(ServiceBusConnection serviceBusConnection, string entityPath, Azure.Messaging.ServiceBus.ServiceBusReceiveMode receiveMode = Azure.Messaging.ServiceBus.ServiceBusReceiveMode.PeekLock, RetryPolicy retryPolicy = null, int prefetchCount = 0)
        {
            var options = new Azure.Messaging.ServiceBus.ServiceBusReceiverOptions
            {
                ReceiveMode = receiveMode,
                PrefetchCount = prefetchCount
            };
            this.receiver = serviceBusConnection.GetSendViaEntityScopedClient(entityPath).CreateReceiver(entityPath, options);
        }

        public async Task<Message> ReceiveAsync(TimeSpan serverWaitTime)
        {
            return await this.receiver.ReceiveMessageAsync(serverWaitTime);
        }

        public async Task RenewLockAsync(Message message)
        {
            await this.receiver.RenewMessageLockAsync(message);
        }

        public async Task CompleteAsync(Message message)
        {
            await this.receiver.CompleteMessageAsync(message);
        }

        public async Task CloseAsync()
        {
            await this.receiver.CloseAsync();
        }

        public async Task AbandonAsync(Message message)
        {
            await this.receiver.AbandonMessageAsync(message);
        }

    }

    /// <inheritdoc />
    public class QueueClient
    {
        private readonly Azure.Messaging.ServiceBus.ServiceBusSender sender;

        public QueueClient(ServiceBusConnection serviceBusConnection, string entityPath)
        {
            sender = serviceBusConnection.GetSendViaEntityScopedClient(entityPath).CreateSender(entityPath);
        }

        public async Task SendAsync(List<Message> messageList)
        {
            await sender.SendMessagesAsync(messageList.Select(m => (Azure.Messaging.ServiceBus.ServiceBusMessage)m).ToList());
        }

        public async Task SendAsync(Message message)
        {
            await sender.SendMessageAsync((Azure.Messaging.ServiceBus.ServiceBusMessage)message);
        }

    }

    public class QueueDescription
    {
        private readonly Union<Azure.Messaging.ServiceBus.Administration.QueueProperties, Azure.Messaging.ServiceBus.Administration.QueueRuntimeProperties> propertiesUnion;

        public QueueDescription(Azure.Messaging.ServiceBus.Administration.QueueProperties queueProperties)
        {
            this.propertiesUnion = new Union<Azure.Messaging.ServiceBus.Administration.QueueProperties, Azure.Messaging.ServiceBus.Administration.QueueRuntimeProperties>.Case1(queueProperties);
        }

        public QueueDescription(Azure.Messaging.ServiceBus.Administration.QueueRuntimeProperties queueRuntimeProperties)
        {
            this.propertiesUnion = new Union<Azure.Messaging.ServiceBus.Administration.QueueProperties, Azure.Messaging.ServiceBus.Administration.QueueRuntimeProperties>.Case2(queueRuntimeProperties);
        }

        public long MessageCount => this.propertiesUnion.Match(
            _ => throw NotRuntimePropertiesGetException(),
            runtimeProps => runtimeProps.TotalMessageCount);

        public string Path => this.propertiesUnion.Match(
            props => props.Name,
            runtimeProps => runtimeProps.Name);

        public int MaxDeliveryCount => this.propertiesUnion.Match(
            props => props.MaxDeliveryCount,
            _ => throw RuntimePropertiesGetException());

        public long SizeInBytes => this.propertiesUnion.Match(
            _ => throw NotRuntimePropertiesGetException(),
            runtimeProps => runtimeProps.SizeInBytes);

        private static Exception RuntimePropertiesGetException()
        {
            return new InvalidOperationException($"The property cannot be accessed because the underlying object is {nameof(Azure.Messaging.ServiceBus.Administration.QueueRuntimeProperties)}");
        }

        private static Exception NotRuntimePropertiesGetException()
        {
            return new InvalidOperationException($"The property cannot be accessed because the underlying object is {nameof(Azure.Messaging.ServiceBus.Administration.QueueProperties)}");
        }
    }

    /// <inheritdoc />
    public class ManagementClient : Azure.Messaging.ServiceBus.Administration.ServiceBusAdministrationClient
    {
        public ManagementClient(string connectionString)
            : base(connectionString)
        {
        }

        public ManagementClient(string endpoint, Azure.Core.TokenCredential tokenCredential)
            : base(endpoint, tokenCredential)
        {
        }

        public async Task<QueueDescription> GetQueueRuntimeInfoAsync(string name)
        {
            var response = await this.GetQueueRuntimePropertiesAsync(name);
            return new QueueDescription(response.Value);
        }

        public async Task<IEnumerable<QueueDescription>> GetQueuesAsync()
        {
            List<QueueDescription> queueDescriptions = new List<QueueDescription>();
            await foreach (var queueProperties in base.GetQueuesAsync())
            {
                queueDescriptions.Add(new QueueDescription(queueProperties));
            }

            return queueDescriptions;
        }
    }

    public class SessionClient
    {
        private readonly Azure.Messaging.ServiceBus.ServiceBusClient serviceBusClient;
        private readonly string entityPath;
        private readonly Azure.Messaging.ServiceBus.ServiceBusReceiveMode receiveMode;

        public SessionClient(ServiceBusConnection serviceBusConnection, string entityPath, Azure.Messaging.ServiceBus.ServiceBusReceiveMode receiveMode)
        {
            this.entityPath = entityPath;
            this.receiveMode = receiveMode;
            this.serviceBusClient = serviceBusConnection.GetSendViaEntityScopedClient(entityPath);
        }

        public async Task<IMessageSession> AcceptMessageSessionAsync(TimeSpan operationTimeout)
        {
            try
            {
                var options = new Azure.Messaging.ServiceBus.ServiceBusSessionReceiverOptions
                {
                    ReceiveMode = receiveMode,
                };
                return new IMessageSession(await this.serviceBusClient.AcceptNextSessionAsync(entityPath, options));
            }
            catch (Azure.Messaging.ServiceBus.ServiceBusException e) when (e.Reason.Equals(Azure.Messaging.ServiceBus.ServiceBusFailureReason.ServiceTimeout))
            {
                return null;
            }
        }

        public Task CloseAsync()
        {
            return Task.CompletedTask;
        }

    }

}
#pragma warning restore 1591
