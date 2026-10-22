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

namespace DurableTask.ServiceBus.Tests
{
    using System;
    using System.Diagnostics;
    using System.Reflection;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Messaging.ServiceBus;
    using DurableTask.ServiceBus.Common.Abstraction;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class SessionClientTests
    {
        [TestMethod]
        public async Task AcceptMessageSessionAsyncReturnsSessionWhenAccepted()
        {
            var client = new FakeServiceBusClient((ct) => Task.FromResult<ServiceBusSessionReceiver>(new FakeSessionReceiver("session-1")));

            IMessageSession session = await CreateSessionClient(client).AcceptMessageSessionAsync(TimeSpan.FromSeconds(30));

            Assert.IsNotNull(session);
            Assert.AreEqual("session-1", session.SessionId);
        }

        [TestMethod]
        public async Task AcceptMessageSessionAsyncReturnsNullWhenOperationTimeoutElapses()
        {
            var client = new FakeServiceBusClient(async (ct) =>
            {
                await Task.Delay(TimeSpan.FromSeconds(10), ct);
                return null;
            });

            Stopwatch timer = Stopwatch.StartNew();
            IMessageSession session = await CreateSessionClient(client).AcceptMessageSessionAsync(TimeSpan.FromMilliseconds(200));
            timer.Stop();

            Assert.IsNull(session);
            Assert.IsTrue(client.LastToken.IsCancellationRequested, "The operation timeout should cancel the accept call.");
            Assert.IsTrue(timer.Elapsed < TimeSpan.FromSeconds(5), $"Accept should return shortly after the operation timeout, but took {timer.Elapsed}.");
        }

        [TestMethod]
        public async Task AcceptMessageSessionAsyncReturnsNullOnServiceTimeout()
        {
            var client = new FakeServiceBusClient((ct) =>
                throw new ServiceBusException("No session available.", ServiceBusFailureReason.ServiceTimeout));

            IMessageSession session = await CreateSessionClient(client).AcceptMessageSessionAsync(TimeSpan.FromSeconds(30));

            Assert.IsNull(session);
        }

        [TestMethod]
        public async Task AcceptMessageSessionAsyncWithNonPositiveTimeoutDoesNotCancel()
        {
            var client = new FakeServiceBusClient(async (ct) =>
            {
                await Task.Delay(200);
                Assert.IsFalse(ct.IsCancellationRequested, "A non-positive timeout should not cancel the accept call.");
                throw new ServiceBusException("No session available.", ServiceBusFailureReason.ServiceTimeout);
            });

            IMessageSession session = await CreateSessionClient(client).AcceptMessageSessionAsync(TimeSpan.Zero);

            Assert.IsNull(session);
        }

        [TestMethod]
        public async Task AcceptMessageSessionAsyncPropagatesCancellationNotCausedByTimeout()
        {
            var client = new FakeServiceBusClient((ct) => throw new OperationCanceledException());

            await Assert.ThrowsExceptionAsync<OperationCanceledException>(
                () => CreateSessionClient(client).AcceptMessageSessionAsync(TimeSpan.FromSeconds(30)));
        }

        [TestMethod]
        public async Task AcceptMessageSessionAsyncPropagatesOtherServiceBusErrors()
        {
            var client = new FakeServiceBusClient((ct) =>
                throw new ServiceBusException("Entity not found.", ServiceBusFailureReason.MessagingEntityNotFound));

            ServiceBusException exception = await Assert.ThrowsExceptionAsync<ServiceBusException>(
                () => CreateSessionClient(client).AcceptMessageSessionAsync(TimeSpan.FromSeconds(30)));
            Assert.AreEqual(ServiceBusFailureReason.MessagingEntityNotFound, exception.Reason);
        }

        static SessionClient CreateSessionClient(ServiceBusClient serviceBusClient)
        {
#if NETFRAMEWORK
            var sessionClient = (SessionClient)System.Runtime.Serialization.FormatterServices.GetUninitializedObject(typeof(SessionClient));
#else
            var sessionClient = (SessionClient)RuntimeHelpers.GetUninitializedObject(typeof(SessionClient));
#endif
            SetField(sessionClient, "serviceBusClient", serviceBusClient);
            SetField(sessionClient, "entityPath", "test-entity");
            SetField(sessionClient, "receiveMode", ServiceBusReceiveMode.PeekLock);
            return sessionClient;
        }

        static void SetField(object target, string name, object value)
        {
            FieldInfo field = typeof(SessionClient).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.IsNotNull(field, $"Field '{name}' was not found on {nameof(SessionClient)}.");
            field.SetValue(target, value);
        }

        sealed class FakeServiceBusClient : ServiceBusClient
        {
            readonly Func<CancellationToken, Task<ServiceBusSessionReceiver>> acceptNextSession;

            public FakeServiceBusClient(Func<CancellationToken, Task<ServiceBusSessionReceiver>> acceptNextSession)
            {
                this.acceptNextSession = acceptNextSession;
            }

            public CancellationToken LastToken { get; private set; }

            public override Task<ServiceBusSessionReceiver> AcceptNextSessionAsync(
                string queueName,
                ServiceBusSessionReceiverOptions options = default,
                CancellationToken cancellationToken = default)
            {
                this.LastToken = cancellationToken;
                return this.acceptNextSession(cancellationToken);
            }
        }

        sealed class FakeSessionReceiver : ServiceBusSessionReceiver
        {
            readonly string sessionId;

            public FakeSessionReceiver(string sessionId)
            {
                this.sessionId = sessionId;
            }

            public override string SessionId => this.sessionId;
        }
    }
}
