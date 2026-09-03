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

namespace DurableTask.AzureStorage.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.Tracing;
    using System.Linq;
    using System.Reflection;
    using DurableTask.AzureStorage.Logging;
    using DurableTask.Core.Logging;
    using Microsoft.Extensions.Logging;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class LoggingTests
    {
        [TestMethod]
        public void AbandoningMessage_HasExpectedStructuredFieldsAndMessage()
        {
            var logEvent = new LogEvents.AbandoningMessage(
                "test-account",
                "test-hub",
                "TaskScheduled",
                42,
                "message-id",
                "instance-id",
                "execution-id",
                "control-queue",
                17,
                "pop-receipt",
                30,
                "The activity work item could not be processed.");

            var fields = (IReadOnlyDictionary<string, object>)logEvent;
            Assert.AreEqual("test-account", fields["Account"]);
            Assert.AreEqual("test-hub", fields["TaskHub"]);
            Assert.AreEqual("TaskScheduled", fields["EventType"]);
            Assert.AreEqual(42, fields["TaskEventId"]);
            Assert.AreEqual("message-id", fields["MessageId"]);
            Assert.AreEqual("instance-id", fields["InstanceId"]);
            Assert.AreEqual("execution-id", fields["ExecutionId"]);
            Assert.AreEqual("control-queue", fields["PartitionId"]);
            Assert.AreEqual(17L, fields["SequenceNumber"]);
            Assert.AreEqual("pop-receipt", fields["PopReceipt"]);
            Assert.AreEqual(30, fields["VisibilityTimeoutSeconds"]);
            Assert.AreEqual("The activity work item could not be processed.", fields["Details"]);
            Assert.AreEqual(LogLevel.Warning, logEvent.Level);
            Assert.AreEqual(EventIds.AbandoningMessage, logEvent.EventId.Id);
            Assert.AreEqual(nameof(EventIds.AbandoningMessage), logEvent.EventId.Name);
            Assert.AreEqual(
                "instance-id: Abandoning [TaskScheduled#42] message back to control-queue and setting a visibility delay of 30ms: The activity work item could not be processed.",
                ((ILogEvent)logEvent).FormattedMessage);
        }

        [TestMethod]
        public void AbandoningMessage_EventSourceSchemaAppendsDetailsAndUsesVersionEight()
        {
            MethodInfo method = typeof(AnalyticsEventSource).GetMethod(nameof(AnalyticsEventSource.AbandoningMessage));
            EventAttribute eventAttribute = method.GetCustomAttribute<EventAttribute>();
            string[] parameterNames = method.GetParameters().Select(parameter => parameter.Name).ToArray();

            Assert.AreEqual(8, eventAttribute.Version);
            CollectionAssert.AreEqual(
                new[]
                {
                    "Account",
                    "TaskHub",
                    "EventType",
                    "TaskEventId",
                    "MessageId",
                    "InstanceId",
                    "ExecutionId",
                    "PartitionId",
                    "SequenceNumber",
                    "PopReceipt",
                    "VisibilityTimeoutSeconds",
                    "AppName",
                    "ExtensionVersion",
                    "Details",
                },
                parameterNames);
        }

        [TestMethod]
        public void AbandoningMessage_LogHelperPropagatesDetails()
        {
            var logger = new CapturingLogger();
            var logHelper = new LogHelper(logger);

            logHelper.AbandoningMessage(
                "test-account",
                "test-hub",
                "TaskScheduled",
                42,
                "message-id",
                "instance-id",
                "execution-id",
                "control-queue",
                17,
                "pop-receipt",
                30,
                "The orchestration work item could not be processed.");

            var fields = (IReadOnlyDictionary<string, object>)logger.State;
            Assert.AreEqual("The orchestration work item could not be processed.", fields["Details"]);
        }

        sealed class CapturingLogger : ILogger
        {
            public object State { get; private set; }

            public IDisposable BeginScope<TState>(TState state) => NullScope.Instance;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception exception,
                Func<TState, Exception, string> formatter)
            {
                this.State = state;
            }
        }

        sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new NullScope();

            public void Dispose()
            {
            }
        }
    }
}
