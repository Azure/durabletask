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

    /// <inheritdoc />
    public class IMessageSession
    {
        Microsoft.Azure.ServiceBus.IMessageSession session;

        public IMessageSession(Microsoft.Azure.ServiceBus.IMessageSession session)
        {
            this.session = session;
        }

        public string SessionId => this.session.SessionId;

        public DateTime LockedUntilUtc => this.session.LockedUntilUtc;

        public async Task<byte[]> GetStateAsync()
        {
            return await this.session.GetStateAsync();
        }

        public async Task SetStateAsync(byte[] sessionState)
        {
            await this.session.SetStateAsync(sessionState);
        }

        public async Task RenewSessionLockAsync()
        {
            await this.session.RenewSessionLockAsync();
        }

        public async Task AbandonAsync(string lockToken)
        {
            await this.session.AbandonAsync(lockToken);
        }

        public async Task<IList<Message>> ReceiveAsync(int maxMessageCount)
        {
            return (await this.session.ReceiveAsync(maxMessageCount)).Select(x => (Message)x).ToList();
        }

        public async Task CompleteAsync(IEnumerable<string> lockTokens)
        {
            await this.session.CompleteAsync(lockTokens);
        }

        public async Task CloseAsync()
        {
            await this.session.CloseAsync();
        }
    }

    /// <inheritdoc />
    public class Message
    {
        Microsoft.Azure.ServiceBus.Message msg;

        public Message(Microsoft.Azure.ServiceBus.Message msg)
        {
            this.msg = msg;
        }

        public static implicit operator Message(Microsoft.Azure.ServiceBus.Message m)
        {
            return m == null ? null : new Message(m);
        }

        public static implicit operator Microsoft.Azure.ServiceBus.Message(Message m)
        {
            return m.msg;
        }

        public Message()
        {
            this.msg = new Microsoft.Azure.ServiceBus.Message();
        }

        public Message(byte[] serializableObject)
        {
            this.msg = new Microsoft.Azure.ServiceBus.Message(serializableObject);
        }
        

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
    }

    /// <inheritdoc />
    public abstract class RetryPolicy : Microsoft.Azure.ServiceBus.RetryPolicy
    {

    }

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
    }

    /// <inheritdoc />
    public class ServiceBusConnectionStringBuilder : Microsoft.Azure.ServiceBus.ServiceBusConnectionStringBuilder
    {
        public ServiceBusConnectionStringBuilder(string connectionString)
            : base(connectionString)
        {
        }
    }

    /// <inheritdoc />
    public class TokenProvider : Microsoft.Azure.ServiceBus.Primitives.ITokenProvider
    {
        private readonly Microsoft.Azure.ServiceBus.Primitives.TokenProvider tokenProvider;

        public TokenProvider(Microsoft.Azure.ServiceBus.Primitives.TokenProvider t)
        {
            this.tokenProvider = t;
        }

        public static implicit operator TokenProvider(Microsoft.Azure.ServiceBus.Primitives.TokenProvider t)
        {
            return new TokenProvider(t);
        }

        public async Task<Microsoft.Azure.ServiceBus.Primitives.SecurityToken> GetTokenAsync(string appliesTo, TimeSpan timeout)
        {
            return await this.tokenProvider.GetTokenAsync(appliesTo, timeout);
        }

        public static TokenProvider CreateSharedAccessSignatureTokenProvider(string keyName, string sharedAccessKey, TimeSpan tokenTimeToLive)
        {
            return Microsoft.Azure.ServiceBus.Primitives.TokenProvider.CreateSharedAccessSignatureTokenProvider(keyName, sharedAccessKey, tokenTimeToLive);
        }
    }

    /// <inheritdoc />
    public class MessageSender : Microsoft.Azure.ServiceBus.Core.MessageSender
    {
        public MessageSender(ServiceBusConnectionStringBuilder connectionStringBuilder, RetryPolicy retryPolicy = null)
            : base(connectionStringBuilder, retryPolicy)
        {
        }

        public MessageSender(string connectionString, string entityPath, RetryPolicy retryPolicy = null)
            : base(connectionString, entityPath, retryPolicy)
        {
        }

        public MessageSender(string endpoint, string entityPath, Microsoft.Azure.ServiceBus.Primitives.ITokenProvider tokenProvider, Microsoft.Azure.ServiceBus.TransportType transportType = Microsoft.Azure.ServiceBus.TransportType.Amqp, RetryPolicy retryPolicy = null)
            : base(endpoint, entityPath, tokenProvider, transportType, retryPolicy)
        {
        }

        public MessageSender(ServiceBusConnection serviceBusConnection, string entityPath, RetryPolicy retryPolicy = null)
            : base(serviceBusConnection, entityPath, retryPolicy)
        {
        }

        public MessageSender(ServiceBusConnection serviceBusConnection, string entityPath, string viaEntityPath, RetryPolicy retryPolicy = null)
            : base(serviceBusConnection, entityPath, viaEntityPath, retryPolicy)
        {
        }

        public async Task SendAsync(Message message)
        {
            await base.SendAsync(message);
        }

        public async Task SendAsync(IEnumerable<Message> messageList)
        {
            await base.SendAsync(messageList.Select(x => (Microsoft.Azure.ServiceBus.Message)x).ToList());
        }
    }

    /// <inheritdoc />
    public class MessageReceiver : Microsoft.Azure.ServiceBus.Core.MessageReceiver
    {
        public MessageReceiver(Microsoft.Azure.ServiceBus.ServiceBusConnectionStringBuilder connectionStringBuilder, Microsoft.Azure.ServiceBus.ReceiveMode receiveMode = Microsoft.Azure.ServiceBus.ReceiveMode.PeekLock, Microsoft.Azure.ServiceBus.RetryPolicy retryPolicy = null, int prefetchCount = 0)
            : base(connectionStringBuilder, receiveMode, retryPolicy, prefetchCount)
        {
        }

        public MessageReceiver(string connectionString, string entityPath, Microsoft.Azure.ServiceBus.ReceiveMode receiveMode = Microsoft.Azure.ServiceBus.ReceiveMode.PeekLock, Microsoft.Azure.ServiceBus.RetryPolicy retryPolicy = null, int prefetchCount = 0)
            : base(connectionString, entityPath, receiveMode, retryPolicy, prefetchCount)
        {
        }

        public MessageReceiver(string endpoint, string entityPath, Microsoft.Azure.ServiceBus.Primitives.ITokenProvider tokenProvider, Microsoft.Azure.ServiceBus.TransportType transportType = Microsoft.Azure.ServiceBus.TransportType.Amqp, Microsoft.Azure.ServiceBus.ReceiveMode receiveMode = Microsoft.Azure.ServiceBus.ReceiveMode.PeekLock, Microsoft.Azure.ServiceBus.RetryPolicy retryPolicy = null, int prefetchCount = 0)
            : base(endpoint, entityPath, tokenProvider, transportType, receiveMode, retryPolicy, prefetchCount)
        {
        }

        public MessageReceiver(Microsoft.Azure.ServiceBus.ServiceBusConnection serviceBusConnection, string entityPath, Microsoft.Azure.ServiceBus.ReceiveMode receiveMode = Microsoft.Azure.ServiceBus.ReceiveMode.PeekLock, Microsoft.Azure.ServiceBus.RetryPolicy retryPolicy = null, int prefetchCount = 0)
            : base(serviceBusConnection, entityPath, receiveMode, retryPolicy, prefetchCount)
        {
        }
    }

    /// <inheritdoc />
    public class QueueClient : Microsoft.Azure.ServiceBus.QueueClient
    {
        public QueueClient(Microsoft.Azure.ServiceBus.ServiceBusConnectionStringBuilder connectionStringBuilder, Microsoft.Azure.ServiceBus.ReceiveMode receiveMode = Microsoft.Azure.ServiceBus.ReceiveMode.PeekLock, Microsoft.Azure.ServiceBus.RetryPolicy retryPolicy = null)
            : base(connectionStringBuilder, receiveMode, retryPolicy)
        {
        }

        public QueueClient(string connectionString, string entityPath, Microsoft.Azure.ServiceBus.ReceiveMode receiveMode = Microsoft.Azure.ServiceBus.ReceiveMode.PeekLock, Microsoft.Azure.ServiceBus.RetryPolicy retryPolicy = null)
            : base(connectionString, entityPath, receiveMode, retryPolicy)
        {
        }

        public QueueClient(string endpoint, string entityPath, Microsoft.Azure.ServiceBus.Primitives.ITokenProvider tokenProvider, Microsoft.Azure.ServiceBus.TransportType transportType = Microsoft.Azure.ServiceBus.TransportType.Amqp, Microsoft.Azure.ServiceBus.ReceiveMode receiveMode = Microsoft.Azure.ServiceBus.ReceiveMode.PeekLock, Microsoft.Azure.ServiceBus.RetryPolicy retryPolicy = null)
            : base(endpoint, entityPath, tokenProvider, transportType, receiveMode, retryPolicy)
        {
        }

        public QueueClient(Microsoft.Azure.ServiceBus.ServiceBusConnection serviceBusConnection, string entityPath, Microsoft.Azure.ServiceBus.ReceiveMode receiveMode, Microsoft.Azure.ServiceBus.RetryPolicy retryPolicy)
            : base(serviceBusConnection, entityPath, receiveMode, retryPolicy)
        {
        }

        public async Task SendAsync(List<Message> messageList)
        {
            await SendAsync(messageList.Select(x => (Microsoft.Azure.ServiceBus.Message)x).ToList());
        }
    }

    /// <inheritdoc />
    public class ManagementClient : Microsoft.Azure.ServiceBus.Management.ManagementClient
    {
        public ManagementClient(string connectionString)
            : base(connectionString)
        {
        }

        public ManagementClient(string endpoint, Microsoft.Azure.ServiceBus.Primitives.ITokenProvider tokenProvider)
            : base(endpoint, tokenProvider)
        {
        }

        public ManagementClient(Microsoft.Azure.ServiceBus.ServiceBusConnectionStringBuilder connectionStringBuilder, Microsoft.Azure.ServiceBus.Primitives.ITokenProvider tokenProvider = null)
            : base(connectionStringBuilder, tokenProvider)
        {
        }
    }

    public class SessionClient
    {
        Microsoft.Azure.ServiceBus.SessionClient sessionClient;

        public SessionClient(ServiceBusConnection serviceBusConnection, string entityPath, Microsoft.Azure.ServiceBus.ReceiveMode receiveMode)
        {
            this.sessionClient = new Microsoft.Azure.ServiceBus.SessionClient(serviceBusConnection, entityPath, receiveMode);
        }

        public SessionClient(Microsoft.Azure.ServiceBus.SessionClient sessionClient)
        {
            this.sessionClient = sessionClient;
        }

        public static implicit operator SessionClient(Microsoft.Azure.ServiceBus.SessionClient sc)
        {
            return new SessionClient(sc);
        }

        public async Task CloseAsync()
        {
            await this.sessionClient.CloseAsync();
        }

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
    }

}
#pragma warning restore 1591
