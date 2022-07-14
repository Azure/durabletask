#pragma warning disable 1591
namespace DurableTask.ServiceBus.Common.Abstraction;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using System.Xml;

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
internal sealed class DataContractBinarySerializer : XmlObjectSerializer
{
    private readonly DataContractSerializer dataContractSerializer;

    /// <summary>
    /// Initializes a new DataContractBinarySerializer instance
    /// </summary>
    public DataContractBinarySerializer(Type type) => this.dataContractSerializer = new DataContractSerializer(type);

    /// <summary>
    /// Converts from stream to the corresponding object
    /// </summary>
    /// <returns>Object corresponding to the stream</returns>
    /// <remarks>Override the default (Text) and use Binary Xml Reader instead</remarks>
    public override object ReadObject(Stream stream)
     => this.ReadObject(XmlDictionaryReader.CreateBinaryReader(stream, XmlDictionaryReaderQuotas.Max));

    /// <summary>
    /// Serializes the object into the stream
    /// </summary>
    /// <remarks>Override the default (Text) and use Binary Xml Reader instead</remarks>
    public override void WriteObject(Stream stream, object graph)
    {
        if (stream is null)
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
        if (writer is null)
        {
            throw new ArgumentNullException(nameof(writer));
        }

        this.dataContractSerializer.WriteObject(writer, graph);
    }

    /// <summary>
    /// This method simply delegates to the DataContractSerializer implementation
    /// </summary>
    public override bool IsStartObject(XmlDictionaryReader reader)
     => this.dataContractSerializer.IsStartObject(reader);

    /// <summary>
    /// This method simply delegates to the DataContractSerializer implementation
    /// </summary>
    public override object ReadObject(XmlDictionaryReader reader, bool verifyObjectName)
     => this.dataContractSerializer.ReadObject(reader, verifyObjectName);

    /// <summary>
    /// This method simply delegates to the DataContractSerializer implementation
    /// </summary>
    public override void WriteEndObject(XmlDictionaryWriter writer)
     => this.dataContractSerializer.WriteEndObject(writer);

    /// <summary>
    /// This method simply delegates to the DataContractSerializer implementation
    /// </summary>
    public override void WriteObjectContent(XmlDictionaryWriter writer, object graph)
     => this.dataContractSerializer.WriteObjectContent(writer, graph);

    /// <summary>
    /// This method simply delegates to the DataContractSerializer implementation
    /// </summary>
    public override void WriteStartObject(XmlDictionaryWriter writer, object graph)
     => this.dataContractSerializer.WriteStartObject(writer, graph);
}
#endif

#if NETSTANDARD2_0
/// <inheritdoc />
public class IMessageSession
{
    private readonly Microsoft.Azure.ServiceBus.IMessageSession session;

    public IMessageSession(Microsoft.Azure.ServiceBus.IMessageSession session) => this.session = session;

    public string SessionId => this.session.SessionId;

    public DateTime LockedUntilUtc => this.session.LockedUntilUtc;

    public Task<byte[]> GetStateAsync() => this.session.GetStateAsync();

    public Task SetStateAsync(byte[] sessionState) => this.session.SetStateAsync(sessionState);

    public Task RenewSessionLockAsync() => this.session.RenewSessionLockAsync();

    public Task AbandonAsync(string lockToken) => this.session.AbandonAsync(lockToken);

    public async Task<IList<Message>> ReceiveAsync(int maxMessageCount)
     => (await this.session.ReceiveAsync(maxMessageCount)).Select(x => (Message)x).ToList();

    public Task CompleteAsync(IEnumerable<string> lockTokens) => this.session.CompleteAsync(lockTokens);

    public Task CloseAsync() => this.session.CloseAsync();
#else
public class IMessageSession
{
    private readonly MessageSession session;

    public IMessageSession(MessageSession session) => this.session = session;

    public static implicit operator IMessageSession(MessageSession s) => new IMessageSession(s);

    public string SessionId => this.session.SessionId;

    public DateTime LockedUntilUtc => this.session.LockedUntilUtc;

    public async Task<byte[]> GetStateAsync()
    {
        Stream state = await this.session.GetStateAsync();
        if (state is null)
            return null;
        using (var ms = new MemoryStream())
        {
            state.CopyTo(ms);
            return ms.ToArray();
        }
    }

    public async Task SetStateAsync(byte[] sessionState)
    {
        if (sessionState is null)
        {
            await this.session.SetStateAsync(null);
            return;
        }

        var stream = new MemoryStream();
        stream.Write(sessionState, 0, sessionState.Length);
        await this.session.SetStateAsync(stream);
    }

    public Task RenewSessionLockAsync() => this.session.RenewLockAsync();

    public Task AbandonAsync(string lockToken) => this.session.AbandonAsync(Guid.Parse(lockToken));

    public async Task<IList<Message>> ReceiveAsync(int maxMessageCount)
     => (await this.session.ReceiveBatchAsync(maxMessageCount)).Select(x => (Message)x).ToList();

    public Task CompleteAsync(IEnumerable<string> lockTokens)
     => this.session.CompleteBatchAsync(lockTokens.Select(Guid.Parse));

    public Task CloseAsync() => this.session.CloseAsync();
#endif

}

#if NETSTANDARD2_0
/// <inheritdoc />
public class Message
{
    private readonly Microsoft.Azure.ServiceBus.Message msg;

    public Message(Microsoft.Azure.ServiceBus.Message msg) => this.msg = msg;

    public static implicit operator Message(Microsoft.Azure.ServiceBus.Message m) => m is null ? null : new Message(m);

    public static implicit operator Microsoft.Azure.ServiceBus.Message(Message m) => m.msg;

    public Message() => this.msg = new Microsoft.Azure.ServiceBus.Message();

    public Message(byte[] serializableObject) => this.msg = new Microsoft.Azure.ServiceBus.Message(serializableObject);


    public string MessageId
    {
        get => this.msg?.MessageId;
        set => this.msg.MessageId = value;
    }

    public DateTime ScheduledEnqueueTimeUtc
    {
        get => this.msg.ScheduledEnqueueTimeUtc;
        set => this.msg.ScheduledEnqueueTimeUtc = value;
    }

    public Microsoft.Azure.ServiceBus.Message.SystemPropertiesCollection SystemProperties => this.msg.SystemProperties;

    public IDictionary<string, object> UserProperties => this.msg.UserProperties;

    public byte[] Body => this.msg.Body;

    public string SessionId
    {
        get => this.msg?.SessionId;
        set => this.msg.SessionId = value;
    }

#else

public class Message : IDisposable
{
    private BrokeredMessage brokered;

    public Message(BrokeredMessage brokered)
    {
        this.brokered = brokered;
        this.SystemProperties = new SystemPropertiesCollection(this.brokered);
    }

    public static implicit operator Message(BrokeredMessage b) => b is null ? null : new Message(b);

    public static implicit operator BrokeredMessage(Message m) => m.brokered;

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

    public T GetBody<T>() => this.brokered.GetBody<T>();

    /// <inheritdoc />
    public void Dispose()
    {
        this.brokered.Dispose();
        this.brokered = null;
    }
}

public class SystemPropertiesCollection
{
    private readonly BrokeredMessage brokered;

    /// <inheritdoc />
    public SystemPropertiesCollection(BrokeredMessage brokered) => this.brokered = brokered;

    /// <summary>Gets the lock token for the current message.</summary>
    /// <remarks>
    ///   The lock token is a reference to the lock that is being held by the broker in <see cref="F:Microsoft.Azure.ServiceBus.ReceiveMode.PeekLock" /> mode.
    ///   Locks are used to explicitly settle messages as explained in the <a href="https://docs.microsoft.com/azure/service-bus-messaging/message-transfers-locks-settlement">product documentation in more detail</a>.
    ///   The token can also be used to pin the lock permanently through the <a href="https://docs.microsoft.com/azure/service-bus-messaging/message-deferral">Deferral API</a> and, with that, take the message out of the
    ///   regular delivery state flow. This property is read-only.
    /// </remarks>
    public Guid LockToken => this.brokered.LockToken;

    /// <summary>Get the current delivery count.</summary>
    /// <value>This value starts at 1.</value>
    /// <remarks>
    ///    Number of deliveries that have been attempted for this message. The count is incremented when a message lock expires,
    ///    or the message is explicitly abandoned by the receiver. This property is read-only.
    /// </remarks>
    public int DeliveryCount => this.brokered.DeliveryCount;

    /// <summary>Gets the date and time in UTC until which the message will be locked in the queue/subscription.</summary>
    /// <value>The date and time until which the message will be locked in the queue/subscription.</value>
    /// <remarks>
    /// 	For messages retrieved under a lock (peek-lock receive mode, not pre-settled) this property reflects the UTC
    ///     instant until which the message is held locked in the queue/subscription. When the lock expires, the <see cref="P:Microsoft.Azure.ServiceBus.Message.SystemPropertiesCollection.DeliveryCount" />
    ///     is incremented and the message is again available for retrieval. This property is read-only.
    /// </remarks>
    public DateTime LockedUntilUtc => this.brokered.LockedUntilUtc;

    /// <summary>Gets the unique number assigned to a message by Service Bus.</summary>
    /// <remarks>
    ///     The sequence number is a unique 64-bit integer assigned to a message as it is accepted
    ///     and stored by the broker and functions as its true identifier. For partitioned entities,
    ///     the topmost 16 bits reflect the partition identifier. Sequence numbers monotonically increase.
    ///     They roll over to 0 when the 48-64 bit range is exhausted. This property is read-only.
    /// </remarks>
    public long SequenceNumber => this.brokered.SequenceNumber;

    /// <summary>Gets or sets the date and time of the sent time in UTC.</summary>
    /// <value>The enqueue time in UTC. </value>
    /// <remarks>
    ///    The UTC instant at which the message has been accepted and stored in the entity.
    ///    This value can be used as an authoritative and neutral arrival time indicator when
    ///    the receiver does not want to trust the sender's clock. This property is read-only.
    /// </remarks>
    public DateTime EnqueuedTimeUtc => this.brokered.EnqueuedTimeUtc;

#endif
}

#if NETSTANDARD2_0
/// <inheritdoc />
public abstract class RetryPolicy : Microsoft.Azure.ServiceBus.RetryPolicy
{
#else

public class RetryPolicy
{
    private readonly Microsoft.ServiceBus.RetryPolicy policy;

    public RetryPolicy(Microsoft.ServiceBus.RetryPolicy policy) => this.policy = policy;

    public static implicit operator RetryPolicy(Microsoft.ServiceBus.RetryPolicy rp) => new RetryPolicy(rp);

    public static implicit operator Microsoft.ServiceBus.RetryPolicy(RetryPolicy rp) => rp.policy;

    public static RetryPolicy Default => Microsoft.ServiceBus.RetryPolicy.Default;

#endif
}

#if NETSTANDARD2_0
/// <inheritdoc />
public class ServiceBusConnection : Microsoft.Azure.ServiceBus.ServiceBusConnection
{
    public ServiceBusConnection(ServiceBusConnectionStringBuilder connectionStringBuilder)
        : base(connectionStringBuilder)
    {
    }

    public ServiceBusConnection(string namespaceConnectionString)
        : base(namespaceConnectionString)
    {
    }

    public ServiceBusConnection(string namespaceConnectionString, Microsoft.Azure.ServiceBus.RetryPolicy retryPolicy = null)
        : base(namespaceConnectionString, retryPolicy)
    {
    }

    [Obsolete]
    public ServiceBusConnection(string namespaceConnectionString, TimeSpan operationTimeout, Microsoft.Azure.ServiceBus.RetryPolicy retryPolicy = null)
        : base(namespaceConnectionString, operationTimeout, retryPolicy)
    {
    }

    public ServiceBusConnection(string endpoint, Microsoft.Azure.ServiceBus.TransportType transportType, Microsoft.Azure.ServiceBus.RetryPolicy retryPolicy = null)
        : base(endpoint, transportType, retryPolicy)
    {
    }
#else
public class ServiceBusConnection
{
    public ServiceBusConnection(ServiceBusConnectionStringBuilder connectionStringBuilder)
     => ConnectionString = connectionStringBuilder.ToString();

    public string ConnectionString { get; private set; }

    public TokenProvider TokenProvider { get; set; }

#endif
}

#if NETSTANDARD2_0
/// <inheritdoc />
public class ServiceBusConnectionStringBuilder : Microsoft.Azure.ServiceBus.ServiceBusConnectionStringBuilder
{
    public ServiceBusConnectionStringBuilder(string connectionString)
        : base(connectionString)
    {
    }
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
/// <inheritdoc />
public class TokenProvider : Microsoft.Azure.ServiceBus.Primitives.ITokenProvider
{
    private readonly Microsoft.Azure.ServiceBus.Primitives.TokenProvider tokenProvider;

    public TokenProvider(Microsoft.Azure.ServiceBus.Primitives.TokenProvider t) => this.tokenProvider = t;

    public static implicit operator TokenProvider(Microsoft.Azure.ServiceBus.Primitives.TokenProvider t)
     => new TokenProvider(t);

    public async Task<Microsoft.Azure.ServiceBus.Primitives.SecurityToken> GetTokenAsync(string appliesTo, TimeSpan timeout)
     => await this.tokenProvider.GetTokenAsync(appliesTo, timeout);

    public static TokenProvider CreateSharedAccessSignatureTokenProvider(
        string keyName, string sharedAccessKey, TimeSpan tokenTimeToLive)
     => Microsoft.Azure.ServiceBus.Primitives.
        TokenProvider.CreateSharedAccessSignatureTokenProvider(keyName, sharedAccessKey, tokenTimeToLive);
#else
public class TokenProvider
{
    private readonly Microsoft.ServiceBus.TokenProvider tokenProvider;

    public TokenProvider(Microsoft.ServiceBus.TokenProvider t) => this.tokenProvider = t;

    public static implicit operator TokenProvider(Microsoft.ServiceBus.TokenProvider t) => new TokenProvider(t);

    public static implicit operator Microsoft.ServiceBus.TokenProvider(TokenProvider t) => t.tokenProvider;

    public static TokenProvider CreateSharedAccessSignatureTokenProvider(string keyName, string sharedAccessKey, TimeSpan tokenTimeToLive)
     => Microsoft.ServiceBus.
        TokenProvider.CreateSharedAccessSignatureTokenProvider(keyName, sharedAccessKey, tokenTimeToLive);
#endif
}

#if NETSTANDARD2_0
/// <inheritdoc />
public class MessageSender : Microsoft.Azure.ServiceBus.Core.MessageSender
{
    public MessageSender(ServiceBusConnectionStringBuilder connectionStringBuilder, RetryPolicy retryPolicy = null)
        : base(connectionStringBuilder, retryPolicy) { }

    public MessageSender(string connectionString, string entityPath, RetryPolicy retryPolicy = null)
        : base(connectionString, entityPath, retryPolicy) { }

    public MessageSender(string endpoint, string entityPath, Microsoft.Azure.ServiceBus.Primitives.ITokenProvider tokenProvider, Microsoft.Azure.ServiceBus.TransportType transportType = Microsoft.Azure.ServiceBus.TransportType.Amqp, RetryPolicy retryPolicy = null)
        : base(endpoint, entityPath, tokenProvider, transportType, retryPolicy) { }

    public MessageSender(ServiceBusConnection serviceBusConnection, string entityPath, RetryPolicy retryPolicy = null)
        : base(serviceBusConnection, entityPath, retryPolicy) { }

    public MessageSender(ServiceBusConnection serviceBusConnection, string entityPath, string viaEntityPath, RetryPolicy retryPolicy = null)
        : base(serviceBusConnection, entityPath, viaEntityPath, retryPolicy) { }

    public Task SendAsync(Message message) => base.SendAsync(message);

    public async Task SendAsync(IEnumerable<Message> messageList)
     => await base.SendAsync(messageList.Select(x => (Microsoft.Azure.ServiceBus.Message)x).ToList());


#else
public class MessageSender
{
    private readonly Microsoft.ServiceBus.Messaging.MessageSender msgSender;

    public MessageSender(ServiceBusConnection serviceBusConnection, string transferDestinationEntityPath, string viaEntityPath)
     => this.msgSender = BuildMessagingFactory(serviceBusConnection)
                         .CreateMessageSender(transferDestinationEntityPath, viaEntityPath);

    public MessageSender(ServiceBusConnection serviceBusConnection, string entityPath, RetryPolicy retryPolicy = null)
     => this.msgSender = BuildMessagingFactory(serviceBusConnection)
                         .CreateMessageSender(entityPath);

    private static MessagingFactory BuildMessagingFactory(ServiceBusConnection serviceBusConnection)
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

    public MessageSender(Microsoft.ServiceBus.Messaging.MessageSender msgSender) => this.msgSender = msgSender;

    public static implicit operator MessageSender(Microsoft.ServiceBus.Messaging.MessageSender ms) => new MessageSender(ms);

    public Task SendAsync(Message message) => this.msgSender.SendAsync(message);

    public async Task SendAsync(IEnumerable<Message> messages)
     => await this.msgSender.SendBatchAsync(messages.Select(x => (BrokeredMessage)x));

    public Task CloseAsync() => this.msgSender.CloseAsync();

#endif
}

#if NETSTANDARD2_0
/// <inheritdoc />
public class MessageReceiver : Microsoft.Azure.ServiceBus.Core.MessageReceiver
{
    public MessageReceiver(Microsoft.Azure.ServiceBus.ServiceBusConnectionStringBuilder connectionStringBuilder, Microsoft.Azure.ServiceBus.ReceiveMode receiveMode = Microsoft.Azure.ServiceBus.ReceiveMode.PeekLock, Microsoft.Azure.ServiceBus.RetryPolicy retryPolicy = null, int prefetchCount = 0)
        : base(connectionStringBuilder, receiveMode, retryPolicy, prefetchCount) { }

    public MessageReceiver(string connectionString, string entityPath, Microsoft.Azure.ServiceBus.ReceiveMode receiveMode = Microsoft.Azure.ServiceBus.ReceiveMode.PeekLock, Microsoft.Azure.ServiceBus.RetryPolicy retryPolicy = null, int prefetchCount = 0)
        : base(connectionString, entityPath, receiveMode, retryPolicy, prefetchCount) { }

    public MessageReceiver(string endpoint, string entityPath, Microsoft.Azure.ServiceBus.Primitives.ITokenProvider tokenProvider, Microsoft.Azure.ServiceBus.TransportType transportType = Microsoft.Azure.ServiceBus.TransportType.Amqp, Microsoft.Azure.ServiceBus.ReceiveMode receiveMode = Microsoft.Azure.ServiceBus.ReceiveMode.PeekLock, Microsoft.Azure.ServiceBus.RetryPolicy retryPolicy = null, int prefetchCount = 0)
        : base(endpoint, entityPath, tokenProvider, transportType, receiveMode, retryPolicy, prefetchCount) { }

    public MessageReceiver(Microsoft.Azure.ServiceBus.ServiceBusConnection serviceBusConnection, string entityPath, Microsoft.Azure.ServiceBus.ReceiveMode receiveMode = Microsoft.Azure.ServiceBus.ReceiveMode.PeekLock, Microsoft.Azure.ServiceBus.RetryPolicy retryPolicy = null, int prefetchCount = 0)
        : base(serviceBusConnection, entityPath, receiveMode, retryPolicy, prefetchCount) { }

#else
public class MessageReceiver
{
    private readonly Microsoft.ServiceBus.Messaging.MessageReceiver msgReceiver;

    public MessageReceiver(ServiceBusConnection serviceBusConnection, string entityPath)
     => this.msgReceiver = BuildMessagingFactory(serviceBusConnection).CreateMessageReceiver(entityPath);

    private static MessagingFactory BuildMessagingFactory(ServiceBusConnection serviceBusConnection)
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

    public MessageReceiver(Microsoft.ServiceBus.Messaging.MessageReceiver msgReceiver) => this.msgReceiver = msgReceiver;

    public static implicit operator MessageReceiver(Microsoft.ServiceBus.Messaging.MessageReceiver mr)
     => new MessageReceiver(mr);

    public Task AbandonAsync(Guid lockToken) => this.msgReceiver.AbandonAsync(lockToken);

    public Task CloseAsync() => this.msgReceiver.CloseAsync();

    public Task CompleteAsync(Guid lockToken) => this.msgReceiver.CompleteAsync(lockToken);

    public async Task<Message> ReceiveAsync(TimeSpan serverWaitTime)
     => await this.msgReceiver.ReceiveAsync(serverWaitTime);

    public Task RenewLockAsync(Message message) => ((BrokeredMessage)message).RenewLockAsync();

#endif
}

#if NETSTANDARD2_0
/// <inheritdoc />
public class QueueClient : Microsoft.Azure.ServiceBus.QueueClient
{
    public QueueClient(Microsoft.Azure.ServiceBus.ServiceBusConnectionStringBuilder connectionStringBuilder, Microsoft.Azure.ServiceBus.ReceiveMode receiveMode = Microsoft.Azure.ServiceBus.ReceiveMode.PeekLock, Microsoft.Azure.ServiceBus.RetryPolicy retryPolicy = null)
        : base(connectionStringBuilder, receiveMode, retryPolicy) { }

    public QueueClient(string connectionString, string entityPath, Microsoft.Azure.ServiceBus.ReceiveMode receiveMode = Microsoft.Azure.ServiceBus.ReceiveMode.PeekLock, Microsoft.Azure.ServiceBus.RetryPolicy retryPolicy = null)
        : base(connectionString, entityPath, receiveMode, retryPolicy) { }

    public QueueClient(string endpoint, string entityPath, Microsoft.Azure.ServiceBus.Primitives.ITokenProvider tokenProvider, Microsoft.Azure.ServiceBus.TransportType transportType = Microsoft.Azure.ServiceBus.TransportType.Amqp, Microsoft.Azure.ServiceBus.ReceiveMode receiveMode = Microsoft.Azure.ServiceBus.ReceiveMode.PeekLock, Microsoft.Azure.ServiceBus.RetryPolicy retryPolicy = null)
        : base(endpoint, entityPath, tokenProvider, transportType, receiveMode, retryPolicy) { }

    public QueueClient(Microsoft.Azure.ServiceBus.ServiceBusConnection serviceBusConnection, string entityPath, Microsoft.Azure.ServiceBus.ReceiveMode receiveMode, Microsoft.Azure.ServiceBus.RetryPolicy retryPolicy)
        : base(serviceBusConnection, entityPath, receiveMode, retryPolicy) { }

    public async Task SendAsync(List<Message> messageList)
     => await SendAsync(messageList.Select(x => (Microsoft.Azure.ServiceBus.Message)x).ToList());

#else
public class QueueClient
{
    private readonly Microsoft.ServiceBus.Messaging.QueueClient queueClient;

    public QueueClient(ServiceBusConnection serviceBusConnection, string entityPath, ReceiveMode receiveMode, RetryPolicy retryPolicy)
    {
        var factory = BuildMessagingFactory(serviceBusConnection);
        factory.RetryPolicy = retryPolicy;
        this.queueClient = factory.CreateQueueClient(entityPath, receiveMode);
    }

    private static MessagingFactory BuildMessagingFactory(ServiceBusConnection serviceBusConnection)
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

    public QueueClient(Microsoft.ServiceBus.Messaging.QueueClient queueClient) => this.queueClient = queueClient;

    public static implicit operator QueueClient(Microsoft.ServiceBus.Messaging.QueueClient qc) => new QueueClient(qc);

    public Task SendAsync(Message message) => this.queueClient.SendAsync(message);

    public async Task SendAsync(IList<Message> messageList)
     => await this.queueClient.SendBatchAsync(messageList.Select(x => (BrokeredMessage)x));

    public Task CloseAsync() => this.queueClient.CloseAsync();

    public async Task<IMessageSession> AcceptMessageSessionAsync(TimeSpan operationTimeout)
     => await this.queueClient.AcceptMessageSessionAsync(operationTimeout);

#endif
}

#if NETSTANDARD2_0
/// <inheritdoc />
public class ManagementClient : Microsoft.Azure.ServiceBus.Management.ManagementClient
{
    public ManagementClient(string connectionString)
        : base(connectionString) { }

    public ManagementClient(string endpoint, Microsoft.Azure.ServiceBus.Primitives.ITokenProvider tokenProvider)
        : base(endpoint, tokenProvider) { }

    public ManagementClient(Microsoft.Azure.ServiceBus.ServiceBusConnectionStringBuilder connectionStringBuilder, Microsoft.Azure.ServiceBus.Primitives.ITokenProvider tokenProvider = null)
        : base(connectionStringBuilder, tokenProvider) { }
#else
public class ManagementClient
{
    private readonly Microsoft.ServiceBus.NamespaceManager manager;

    public ManagementClient(string connectionString)
     => this.manager = Microsoft.ServiceBus.NamespaceManager.CreateFromConnectionString(connectionString);

    public ManagementClient(Microsoft.ServiceBus.NamespaceManager manager) => this.manager = manager;

    public static implicit operator ManagementClient(Microsoft.ServiceBus.NamespaceManager nm) => new ManagementClient(nm);

    public Task<QueueDescription> CreateQueueAsync(QueueDescription queueDescription) => this.manager.CreateQueueAsync(queueDescription);

    public Task DeleteQueueAsync(string queuePath) => this.manager.DeleteQueueAsync(queuePath);

    public Task<QueueDescription> GetQueueRuntimeInfoAsync(string queuePath) => this.manager.GetQueueAsync(queuePath);

    public Task<IEnumerable<QueueDescription>> GetQueuesAsync() => this.manager.GetQueuesAsync();
#endif
}

#if NETSTANDARD2_0
public class SessionClient
{
    private readonly Microsoft.Azure.ServiceBus.SessionClient sessionClient;

    public SessionClient(ServiceBusConnection serviceBusConnection, string entityPath, Microsoft.Azure.ServiceBus.ReceiveMode receiveMode)
     => this.sessionClient = new Microsoft.Azure.ServiceBus.SessionClient(serviceBusConnection, entityPath, receiveMode);

    public SessionClient(Microsoft.Azure.ServiceBus.SessionClient sessionClient) => this.sessionClient = sessionClient;

    public static implicit operator SessionClient(Microsoft.Azure.ServiceBus.SessionClient sc) => new SessionClient(sc);

    public Task CloseAsync() => this.sessionClient.CloseAsync();

    public async Task<IMessageSession> AcceptMessageSessionAsync(TimeSpan operationTimeout)
    {
        try
        {
            return new IMessageSession(await this.sessionClient.AcceptMessageSessionAsync(operationTimeout));
        }
        catch (Microsoft.Azure.ServiceBus.ServiceBusTimeoutException)
        {
            return null;
        }
    }

#else
public class SessionClient : QueueClient
{
    public SessionClient(ServiceBusConnection serviceBusConnection, string entityPath, ReceiveMode receiveMode)
        : base(serviceBusConnection, entityPath, receiveMode, RetryPolicy.Default) { }

    public SessionClient(Microsoft.ServiceBus.Messaging.QueueClient queueClient)
        : base(queueClient) { }
#endif
}
#pragma warning restore 1591
