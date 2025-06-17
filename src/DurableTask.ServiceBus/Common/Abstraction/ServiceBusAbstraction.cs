#pragma warning disable 1591
namespace DurableTask.ServiceBus.Common.Abstraction
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using System.Xml;
    using DurableTask.ServiceBus.Tracking;
#if !NETSTANDARD2_0
    using Microsoft.ServiceBus.Messaging;

    public class DataContractBinarySerializer<T>
    {
        public static readonly XmlObjectSerializer Instance = new DataContractBinarySerializer(typeof(T));
    }

    /// <summary>
    /// This class describes a serializer class used to serialize and deserialize an Object.
    /// This class is almost identical to DataContractSerializer; only difference is that
    /// ReadObject(Stream) and WriteObject(Stream, object) pick Binary Xml Reader/Writer
    /// instead of text.
    /// </summary>
    sealed class DataContractBinarySerializer : XmlObjectSerializer
    {
        readonly DataContractSerializer dataContractSerializer;

        /// <summary>
        /// Initializes a new DataContractBinarySerializer instance
        /// </summary>
        public DataContractBinarySerializer(Type type)
        {
            this.dataContractSerializer = new DataContractSerializer(type);
        }

        /// <summary>
        /// Converts from stream to the corresponding object
        /// </summary>
        /// <returns>Object corresponding to the stream</returns>
        /// <remarks>Override the default (Text) and use Binary Xml Reader instead</remarks>
        public override object ReadObject(Stream stream)
        {
            return this.ReadObject(XmlDictionaryReader.CreateBinaryReader(stream, XmlDictionaryReaderQuotas.Max));
        }

        /// <summary>
        /// Serializes the object into the stream
        /// </summary>
        /// <remarks>Override the default (Text) and use Binary Xml Reader instead</remarks>
        public override void WriteObject(Stream stream, object graph)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            var xmlDictionaryWriter = XmlDictionaryWriter.CreateBinaryWriter(stream, null, null, false);
            this.WriteObject(xmlDictionaryWriter, graph);
            xmlDictionaryWriter.Flush();
        }

        /// <summary>
        /// Serializes the object into the stream using the XmlDictionaryWriter
        /// </summary>
        public override void WriteObject(XmlDictionaryWriter writer, object graph)
        {
            if (writer == null)
            {
                throw new ArgumentNullException(nameof(writer));
            }

            this.dataContractSerializer.WriteObject(writer, graph);
        }

        /// <summary>
        /// This method simply delegates to the DataContractSerializer implementation
        /// </summary>
        public override bool IsStartObject(XmlDictionaryReader reader)
        {
            return this.dataContractSerializer.IsStartObject(reader);
        }

        /// <summary>
        /// This method simply delegates to the DataContractSerializer implementation
        /// </summary>
        public override object ReadObject(XmlDictionaryReader reader, bool verifyObjectName)
        {
            return this.dataContractSerializer.ReadObject(reader, verifyObjectName);
        }

        /// <summary>
        /// This method simply delegates to the DataContractSerializer implementation
        /// </summary>
        public override void WriteEndObject(XmlDictionaryWriter writer)
        {
            this.dataContractSerializer.WriteEndObject(writer);
        }

        /// <summary>
        /// This method simply delegates to the DataContractSerializer implementation
        /// </summary>
        public override void WriteObjectContent(XmlDictionaryWriter writer, object graph)
        {
            this.dataContractSerializer.WriteObjectContent(writer, graph);
        }

        /// <summary>
        /// This method simply delegates to the DataContractSerializer implementation
        /// </summary>
        public override void WriteStartObject(XmlDictionaryWriter writer, object graph)
        {
            this.dataContractSerializer.WriteStartObject(writer, graph);
        }
    }
#endif

#if NETSTANDARD2_0
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
#else
    public class IMessageSession
    {
        MessageSession session;

        public IMessageSession(MessageSession session)
        {
            this.session = session;
        }

        public static implicit operator IMessageSession(MessageSession s)
        {
            return new IMessageSession(s);
        }

        public string SessionId => this.session.SessionId;

        public DateTime LockedUntilUtc => this.session.LockedUntilUtc;

        public async Task<byte[]> GetStateAsync()
        {
            Stream state = await this.session.GetStateAsync();
            if (state == null)
                return null;
            using (var ms = new MemoryStream())
            {
                state.CopyTo(ms);
                return ms.ToArray();
            }
        }

        public async Task SetStateAsync(byte[] sessionState)
        {
            if (sessionState == null)
            {
                await this.session.SetStateAsync(null);
                return;
            }

            var stream = new MemoryStream();
            stream.Write(sessionState, 0, sessionState.Length);
            await this.session.SetStateAsync(stream);
        }

        public async Task RenewSessionLockAsync()
        {
            await this.session.RenewLockAsync();
        }

        public async Task AbandonAsync(Message message)
        {
            await this.session.AbandonAsync(message.SystemProperties.LockToken);
        }

        public async Task<IList<Message>> ReceiveAsync(int maxMessageCount)
        {
            return (await this.session.ReceiveBatchAsync(maxMessageCount)).Select(x => (Message)x).ToList();
        }

        public async Task CompleteAsync(IEnumerable<Message> messages)
        {
            await this.session.CompleteBatchAsync(messages.Select(m => m.SystemProperties.LockToken).ToList());
        }

        public async Task CloseAsync()
        {
            await this.session.CloseAsync();
        }
#endif

    }

#if NETSTANDARD2_0
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
        
#else

    public class Message : IDisposable
    {
        BrokeredMessage brokered;

        public Message(BrokeredMessage brokered)
        {
            this.brokered = brokered;
            this.SystemProperties = new SystemPropertiesCollection(this.brokered);
        }

        public static implicit operator Message(BrokeredMessage b)
        {
            return b == null ? null : new Message(b);
        }

        public static implicit operator BrokeredMessage(Message m)
        {
            return m.brokered;
        }

        public Message()
        {
            this.brokered = new BrokeredMessage();
            this.SystemProperties = new SystemPropertiesCollection(this.brokered);
        }

        public Message(object serializableObject)
        {
            this.brokered = new BrokeredMessage(serializableObject);
            this.SystemProperties = new SystemPropertiesCollection(this.brokered);
        }

        public Message(Stream stream)
        {
            this.brokered = new BrokeredMessage(stream);
            this.SystemProperties = new SystemPropertiesCollection(this.brokered);
        }

        public string MessageId
        {
            get => this.brokered?.MessageId;
            set => this.brokered.MessageId = value;
        }

        public DateTime ScheduledEnqueueTimeUtc
        {
            get => this.brokered.ScheduledEnqueueTimeUtc;
            set => this.brokered.ScheduledEnqueueTimeUtc = value;
        }

        public SystemPropertiesCollection SystemProperties { get; private set; }

        public IDictionary<string, object> UserProperties => this.brokered.Properties;

        public string SessionId
        {
            get => this.brokered?.SessionId;
            set => this.brokered.SessionId = value;
        }

        public T GetBody<T>()
        {
            return this.brokered.GetBody<T>();
        }

        /// <inheritdoc />
        public void Dispose()
        {
            this.brokered.Dispose();
            this.brokered = null;
        }
    }

    public class SystemPropertiesCollection
    {
        readonly BrokeredMessage _brokered;

        /// <inheritdoc />
        public SystemPropertiesCollection(BrokeredMessage brokered)
        {
            this._brokered = brokered;
        }

        /// <summary>Gets the lock token for the current message.</summary>
        /// <remarks>
        ///   The lock token is a reference to the lock that is being held by the broker in <see cref="F:Microsoft.Azure.ServiceBus.ReceiveMode.PeekLock" /> mode.
        ///   Locks are used to explicitly settle messages as explained in the <a href="https://docs.microsoft.com/azure/service-bus-messaging/message-transfers-locks-settlement">product documentation in more detail</a>.
        ///   The token can also be used to pin the lock permanently through the <a href="https://docs.microsoft.com/azure/service-bus-messaging/message-deferral">Deferral API</a> and, with that, take the message out of the
        ///   regular delivery state flow. This property is read-only.
        /// </remarks>
        public Guid LockToken => this._brokered.LockToken;

        /// <summary>Get the current delivery count.</summary>
        /// <value>This value starts at 1.</value>
        /// <remarks>
        ///    Number of deliveries that have been attempted for this message. The count is incremented when a message lock expires,
        ///    or the message is explicitly abandoned by the receiver. This property is read-only.
        /// </remarks>
        public int DeliveryCount => this._brokered.DeliveryCount;

        /// <summary>Gets the date and time in UTC until which the message will be locked in the queue/subscription.</summary>
        /// <value>The date and time until which the message will be locked in the queue/subscription.</value>
        /// <remarks>
        /// 	For messages retrieved under a lock (peek-lock receive mode, not pre-settled) this property reflects the UTC
        ///     instant until which the message is held locked in the queue/subscription. When the lock expires, the <see cref="P:Microsoft.Azure.ServiceBus.Message.SystemPropertiesCollection.DeliveryCount" />
        ///     is incremented and the message is again available for retrieval. This property is read-only.
        /// </remarks>
        public DateTime LockedUntilUtc => this._brokered.LockedUntilUtc;

        /// <summary>Gets the unique number assigned to a message by Service Bus.</summary>
        /// <remarks>
        ///     The sequence number is a unique 64-bit integer assigned to a message as it is accepted
        ///     and stored by the broker and functions as its true identifier. For partitioned entities,
        ///     the topmost 16 bits reflect the partition identifier. Sequence numbers monotonically increase.
        ///     They roll over to 0 when the 48-64 bit range is exhausted. This property is read-only.
        /// </remarks>
        public long SequenceNumber => this._brokered.SequenceNumber;

        /// <summary>Gets or sets the date and time of the sent time in UTC.</summary>
        /// <value>The enqueue time in UTC. </value>
        /// <remarks>
        ///    The UTC instant at which the message has been accepted and stored in the entity.
        ///    This value can be used as an authoritative and neutral arrival time indicator when
        ///    the receiver does not want to trust the sender's clock. This property is read-only.
        /// </remarks>
        public DateTime EnqueuedTimeUtc => this._brokered.EnqueuedTimeUtc;

#endif
    }

#if NETSTANDARD2_0
    /// <inheritdoc />
    public abstract class RetryPolicy : Azure.Messaging.ServiceBus.ServiceBusRetryPolicy
    {
#else

    public class RetryPolicy
    {
        Microsoft.ServiceBus.RetryPolicy policy;

        public RetryPolicy(Microsoft.ServiceBus.RetryPolicy policy)
        {
            this.policy = policy;
        }

        public static implicit operator RetryPolicy(Microsoft.ServiceBus.RetryPolicy rp)
        {
            return new RetryPolicy(rp);
        }

        public static implicit operator Microsoft.ServiceBus.RetryPolicy(RetryPolicy rp)
        {
            return rp.policy;
        }

        public static RetryPolicy Default => Microsoft.ServiceBus.RetryPolicy.Default;

#endif
    }

#if NETSTANDARD2_0
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
#else
    public class ServiceBusConnection
    {
        public ServiceBusConnection(ServiceBusConnectionStringBuilder connectionStringBuilder)
        {
            ConnectionString = connectionStringBuilder.ToString();
        }

        public string ConnectionString { get; private set; }

        public TokenProvider TokenProvider { get; set; }

#endif
    }

#if NETSTANDARD2_0
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
#else
    public class ServiceBusConnectionStringBuilder : Microsoft.ServiceBus.ServiceBusConnectionStringBuilder
    {
        public ServiceBusConnectionStringBuilder(string connectionString)
            : base(connectionString)
        {
        }

        public string SasKeyName => base.SharedAccessKeyName;

        public string SasKey => base.SharedAccessKey;

#endif
    }

#if NETSTANDARD2_0
#else
    public class TokenProvider
    {
        private Microsoft.ServiceBus.TokenProvider tokenProvider;

        public TokenProvider(Microsoft.ServiceBus.TokenProvider t)
        {
            this.tokenProvider = t;
        }

        public static implicit operator TokenProvider(Microsoft.ServiceBus.TokenProvider t)
        {
            return new TokenProvider(t);
        }

        public static implicit operator Microsoft.ServiceBus.TokenProvider(TokenProvider t)
        {
            return t.tokenProvider;
        }

        public static TokenProvider CreateSharedAccessSignatureTokenProvider(string keyName, string sharedAccessKey, TimeSpan tokenTimeToLive)
        {
            return Microsoft.ServiceBus.TokenProvider.CreateSharedAccessSignatureTokenProvider(keyName, sharedAccessKey, tokenTimeToLive);
        }
    }
#endif

#if NETSTANDARD2_0
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


#else
    public class MessageSender
    {
        Microsoft.ServiceBus.Messaging.MessageSender msgSender;

        public MessageSender(ServiceBusConnection serviceBusConnection, string transferDestinationEntityPath, string viaEntityPath)
        {
            this.msgSender = BuildMessagingFactory(serviceBusConnection).CreateMessageSender(transferDestinationEntityPath, viaEntityPath);
        }

        public MessageSender(ServiceBusConnection serviceBusConnection, string entityPath, RetryPolicy retryPolicy = null)
        {
            this.msgSender = BuildMessagingFactory(serviceBusConnection).CreateMessageSender(entityPath);
        }

        static MessagingFactory BuildMessagingFactory(ServiceBusConnection serviceBusConnection)
        {
            var namespaceManager = Microsoft.ServiceBus.NamespaceManager.CreateFromConnectionString(serviceBusConnection.ConnectionString);
            var factory = Microsoft.ServiceBus.Messaging.MessagingFactory.Create(
                namespaceManager.Address.ToString(),
                new MessagingFactorySettings
                {
                    TransportType = TransportType.NetMessaging,
                    TokenProvider = serviceBusConnection.TokenProvider,
                    NetMessagingTransportSettings = new NetMessagingTransportSettings
                    {
                        BatchFlushInterval = TimeSpan.FromMilliseconds(Core.FrameworkConstants.BatchFlushIntervalInMilliSecs)
                    }
                });
            factory.RetryPolicy = Microsoft.ServiceBus.RetryPolicy.Default;
            return factory;
        }

        public MessageSender(Microsoft.ServiceBus.Messaging.MessageSender msgSender)
        {
            this.msgSender = msgSender;
        }

        public static implicit operator MessageSender(Microsoft.ServiceBus.Messaging.MessageSender ms)
        {
            return new MessageSender(ms);
        }

        public async Task SendAsync(Message message)
        {
            await this.msgSender.SendAsync(message);
        }

        public async Task SendAsync(IEnumerable<Message> messages)
        {
            await this.msgSender.SendBatchAsync(messages.Select(x => (BrokeredMessage)x));
        }

        public async Task CloseAsync()
        {
            await this.msgSender.CloseAsync();
        }

#endif
    }

#if NETSTANDARD2_0
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

#else
    public class MessageReceiver
    {
        Microsoft.ServiceBus.Messaging.MessageReceiver msgReceiver;

        public MessageReceiver(ServiceBusConnection serviceBusConnection, string entityPath)
        {
            this.msgReceiver = BuildMessagingFactory(serviceBusConnection).CreateMessageReceiver(entityPath);
        }

        static MessagingFactory BuildMessagingFactory(ServiceBusConnection serviceBusConnection)
        {
            var namespaceManager = Microsoft.ServiceBus.NamespaceManager.CreateFromConnectionString(serviceBusConnection.ConnectionString);
            var factory = Microsoft.ServiceBus.Messaging.MessagingFactory.Create(
                namespaceManager.Address.ToString(),
                new MessagingFactorySettings
                {
                    TransportType = TransportType.NetMessaging,
                    TokenProvider = serviceBusConnection.TokenProvider,
                    NetMessagingTransportSettings = new NetMessagingTransportSettings
                    {
                        BatchFlushInterval = TimeSpan.FromMilliseconds(Core.FrameworkConstants.BatchFlushIntervalInMilliSecs)
                    }
                });
            factory.RetryPolicy = Microsoft.ServiceBus.RetryPolicy.Default;
            return factory;
        }

        public MessageReceiver(Microsoft.ServiceBus.Messaging.MessageReceiver msgReceiver)
        {
            this.msgReceiver = msgReceiver;
        }

        public static implicit operator MessageReceiver(Microsoft.ServiceBus.Messaging.MessageReceiver mr)
        {
            return new MessageReceiver(mr);
        }

        public async Task AbandonAsync(Message message)
        {
            await this.msgReceiver.AbandonAsync(message.SystemProperties.LockToken);
        }

        public async Task CloseAsync()
        {
            await this.msgReceiver.CloseAsync();
        }

        public async Task CompleteAsync(Message message)
        {
            await this.msgReceiver.CompleteAsync(message.SystemProperties.LockToken);
        }

        public async Task<Message> ReceiveAsync(TimeSpan serverWaitTime)
        {
            return await this.msgReceiver.ReceiveAsync(serverWaitTime);
        }

        public async Task RenewLockAsync(Message message)
        {
            await ((BrokeredMessage)message).RenewLockAsync();
        }

#endif
    }

#if NETSTANDARD2_0
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

#else
    public class QueueClient
    {
        Microsoft.ServiceBus.Messaging.QueueClient queueClient;

        public QueueClient(ServiceBusConnection serviceBusConnection, string entityPath, ReceiveMode receiveMode, RetryPolicy retryPolicy)
        {
            var factory = BuildMessagingFactory(serviceBusConnection);
            factory.RetryPolicy = retryPolicy;
            this.queueClient = factory.CreateQueueClient(entityPath, receiveMode);
        }

        static MessagingFactory BuildMessagingFactory(ServiceBusConnection serviceBusConnection)
        {
            var namespaceManager = Microsoft.ServiceBus.NamespaceManager.CreateFromConnectionString(serviceBusConnection.ConnectionString);
            var factory = Microsoft.ServiceBus.Messaging.MessagingFactory.Create(
                namespaceManager.Address.ToString(),
                new MessagingFactorySettings
                {
                    TransportType = TransportType.NetMessaging,
                    TokenProvider = serviceBusConnection.TokenProvider,
                    NetMessagingTransportSettings = new NetMessagingTransportSettings
                    {
                        BatchFlushInterval = TimeSpan.FromMilliseconds(Core.FrameworkConstants.BatchFlushIntervalInMilliSecs)
                    }
                });
            factory.RetryPolicy = Microsoft.ServiceBus.RetryPolicy.Default;
            return factory;
        }

        public QueueClient(Microsoft.ServiceBus.Messaging.QueueClient queueClient)
        {
            this.queueClient = queueClient;
        }

        public static implicit operator QueueClient(Microsoft.ServiceBus.Messaging.QueueClient qc)
        {
            return new QueueClient(qc);
        }

        public async Task SendAsync(Message message)
        {
            await this.queueClient.SendAsync(message);
        }

        public async Task SendAsync(IList<Message> messageList)
        {
            await this.queueClient.SendBatchAsync(messageList.Select(x => (BrokeredMessage)x));
        }

        public async Task CloseAsync()
        {
            await this.queueClient.CloseAsync();
        }

        public async Task<IMessageSession> AcceptMessageSessionAsync(TimeSpan operationTimeout)
        {
            return await this.queueClient.AcceptMessageSessionAsync(operationTimeout);
        }

#endif
    }

#if NETSTANDARD2_0
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
#else
    public class ManagementClient
    {
        Microsoft.ServiceBus.NamespaceManager manager;

        public ManagementClient(string connectionString)
        {
            this.manager = Microsoft.ServiceBus.NamespaceManager.CreateFromConnectionString(connectionString);
        }

        public ManagementClient(Microsoft.ServiceBus.NamespaceManager manager)
        {
            this.manager = manager;
        }

        public static implicit operator ManagementClient(Microsoft.ServiceBus.NamespaceManager nm)
        {
            return new ManagementClient(nm);
        }

        public async Task<QueueDescription> CreateQueueAsync(QueueDescription queueDescription)
        {
            return await this.manager.CreateQueueAsync(queueDescription);
        }

        public async Task DeleteQueueAsync(string queuePath)
        {
            await this.manager.DeleteQueueAsync(queuePath);
        }

        public async Task<QueueDescription> GetQueueRuntimeInfoAsync(string queuePath)
        {
            return await this.manager.GetQueueAsync(queuePath);
        }

        public async Task<IEnumerable<QueueDescription>> GetQueuesAsync()
        {
            return await this.manager.GetQueuesAsync();
        }
#endif
    }

#if NETSTANDARD2_0
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

#else
    public class SessionClient : QueueClient
    {
        public SessionClient(ServiceBusConnection serviceBusConnection, string entityPath, ReceiveMode receiveMode)
            : base(serviceBusConnection, entityPath, receiveMode, RetryPolicy.Default)
        {
        }

        public SessionClient(Microsoft.ServiceBus.Messaging.QueueClient queueClient)
            : base(queueClient)
        {
        }
#endif
    }

}
#pragma warning restore 1591
