//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------
#if !NET462
#nullable enable
namespace DurableTask.Core.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.Tracing;
    using DurableTask.Core.Entities.OperationFormat;
    using DurableTask.Core.Tracing;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using DiagnosticsActivityStatusCode = System.Diagnostics.ActivityStatusCode;
    using TraceActivityStatusCode = DurableTask.Core.Tracing.ActivityStatusCode;

    [TestClass]
    [DoNotParallelize]
    public class TraceHelperTests
    {
        [TestMethod]
        public void EndActivitiesForEntityInvocationResetsSuccessfulStatus()
        {
            var activities = new List<Activity>
            {
                new Activity("entityOperation").Start()
            };
            activities[0].SetStatus(TraceActivityStatusCode.Error, "instrumented error");

            var results = new List<OperationResult>
            {
                new OperationResult()
            };

            TraceHelper.EndActivitiesForProcessingEntityInvocation(activities, results, batchFailureDetails: null);

            Assert.AreEqual(DiagnosticsActivityStatusCode.Ok, activities[0].Status);
        }

        [TestMethod]
        public void EndActivitiesForEntityInvocationMarksFailures()
        {
            var activities = new List<Activity>
            {
                new Activity("entityOperation").Start()
            };

            var failingResults = new List<OperationResult>
            {
                new OperationResult
                {
                    ErrorMessage = "entity failure"
                }
            };

            TraceHelper.EndActivitiesForProcessingEntityInvocation(activities, failingResults, batchFailureDetails: null);

            Assert.AreEqual(DiagnosticsActivityStatusCode.Error, activities[0].Status);
        }

        [TestMethod]
        public void TraceFactoriesRunOnlyWhenRequestedLevelIsEnabled()
        {
            using var listener = new CapturingEventListener();
            listener.Enable(EventLevel.Warning);

            var instance = new OrchestrationInstance
            {
                InstanceId = "instance",
                ExecutionId = "execution",
            };
            int traceFactoryCalls = 0;
            int sessionFactoryCalls = 0;
            int instanceFactoryCalls = 0;

            TraceHelper.Trace(
                TraceEventType.Information,
                "TraceFactory",
                () =>
                {
                    traceFactoryCalls++;
                    return "trace payload";
                });
            TraceHelper.TraceSession(
                TraceEventType.Information,
                "SessionFactory",
                "session",
                () =>
                {
                    sessionFactoryCalls++;
                    return "session payload";
                });
            TraceHelper.TraceInstance(
                TraceEventType.Information,
                "InstanceFactory",
                instance,
                () =>
                {
                    instanceFactoryCalls++;
                    return "instance payload";
                });

            Assert.AreEqual(0, traceFactoryCalls);
            Assert.AreEqual(0, sessionFactoryCalls);
            Assert.AreEqual(0, instanceFactoryCalls);
            Assert.AreEqual(0, listener.Events.Count);

            listener.Enable(EventLevel.Informational);

            TraceHelper.Trace(
                TraceEventType.Information,
                "TraceFactory",
                () =>
                {
                    traceFactoryCalls++;
                    return "trace payload";
                });
            TraceHelper.TraceSession(
                TraceEventType.Information,
                "SessionFactory",
                "session",
                () =>
                {
                    sessionFactoryCalls++;
                    return "session payload";
                });
            TraceHelper.TraceInstance(
                TraceEventType.Information,
                "InstanceFactory",
                instance,
                () =>
                {
                    instanceFactoryCalls++;
                    return "instance payload";
                });

            Assert.AreEqual(1, traceFactoryCalls);
            Assert.AreEqual(1, sessionFactoryCalls);
            Assert.AreEqual(1, instanceFactoryCalls);
            Assert.AreEqual(3, listener.Events.Count);
            AssertEvent(listener.Events[0], "trace payload", "TraceFactory");
            AssertEvent(listener.Events[1], "session payload", "SessionFactory");
            AssertEvent(listener.Events[2], "instance payload", "InstanceFactory");
        }

        [TestMethod]
        public void TraceFormattingRunsOnlyWhenRequestedLevelIsEnabled()
        {
            using var listener = new CapturingEventListener();
            listener.Enable(EventLevel.Warning);

            var instance = new OrchestrationInstance
            {
                InstanceId = "instance",
                ExecutionId = "execution",
            };
            var traceValue = new CountingValue("trace payload");
            var sessionValue = new CountingValue("session payload");
            var instanceValue = new CountingValue("instance payload");

            TraceHelper.Trace(TraceEventType.Information, "TraceFormat", "{0}", traceValue);
            TraceHelper.TraceSession(TraceEventType.Information, "SessionFormat", "session", "{0}", sessionValue);
            TraceHelper.TraceInstance(TraceEventType.Information, "InstanceFormat", instance, "{0}", instanceValue);

            Assert.AreEqual(0, traceValue.ToStringCalls);
            Assert.AreEqual(0, sessionValue.ToStringCalls);
            Assert.AreEqual(0, instanceValue.ToStringCalls);
            Assert.AreEqual(0, listener.Events.Count);

            listener.Enable(EventLevel.Informational);

            TraceHelper.Trace(TraceEventType.Information, "TraceFormat", "{0}", traceValue);
            TraceHelper.TraceSession(TraceEventType.Information, "SessionFormat", "session", "{0}", sessionValue);
            TraceHelper.TraceInstance(TraceEventType.Information, "InstanceFormat", instance, "{0}", instanceValue);

            Assert.AreEqual(1, traceValue.ToStringCalls);
            Assert.AreEqual(1, sessionValue.ToStringCalls);
            Assert.AreEqual(1, instanceValue.ToStringCalls);
            Assert.AreEqual(3, listener.Events.Count);
            AssertEvent(listener.Events[0], "trace payload", "TraceFormat");
            AssertEvent(listener.Events[1], "session payload", "SessionFormat");
            AssertEvent(listener.Events[2], "instance payload", "InstanceFormat");
        }

        [TestMethod]
        public void TraceExceptionPayloadsRunOnlyWhenRequestedLevelIsEnabled()
        {
            using var listener = new CapturingEventListener();
            listener.Enable(EventLevel.Warning);

            var exception = new InvalidOperationException("failure");
            int factoryCalls = 0;
            var formatValue = new CountingValue("format payload");

            Exception factoryResult = TraceHelper.TraceException(
                TraceEventType.Information,
                "ExceptionFactory",
                exception,
                () =>
                {
                    factoryCalls++;
                    return "factory payload";
                });
            Exception formatResult = TraceHelper.TraceException(
                TraceEventType.Information,
                "ExceptionFormat",
                exception,
                "{0}",
                formatValue);

            Assert.AreSame(exception, factoryResult);
            Assert.AreSame(exception, formatResult);
            Assert.AreEqual(0, factoryCalls);
            Assert.AreEqual(0, formatValue.ToStringCalls);
            Assert.AreEqual(0, listener.Events.Count);

            listener.Enable(EventLevel.Informational);

            factoryResult = TraceHelper.TraceException(
                TraceEventType.Information,
                "ExceptionFactory",
                exception,
                () =>
                {
                    factoryCalls++;
                    return "factory payload";
                });
            formatResult = TraceHelper.TraceException(
                TraceEventType.Information,
                "ExceptionFormat",
                exception,
                "{0}",
                formatValue);

            Assert.AreSame(exception, factoryResult);
            Assert.AreSame(exception, formatResult);
            Assert.AreEqual(1, factoryCalls);
            Assert.AreEqual(1, formatValue.ToStringCalls);
            Assert.AreEqual(2, listener.Events.Count);
            AssertEvent(listener.Events[0], FormatExceptionMessage("factory payload", exception), "ExceptionFactory");
            AssertEvent(listener.Events[1], FormatExceptionMessage("format payload", exception), "ExceptionFormat");
        }

        static void AssertEvent(CapturedEvent capturedEvent, string message, string eventType)
        {
            Assert.AreEqual(3, capturedEvent.EventId);
            Assert.AreEqual(7, capturedEvent.PayloadCount);
            Assert.AreEqual(message, capturedEvent.Message);
            Assert.AreEqual(eventType, capturedEvent.EventType);
        }

        static string FormatExceptionMessage(string message, Exception exception)
        {
            return message + "\nException: " + exception.GetType() + " : " + exception.Message + "\n\t" +
                   exception.StackTrace + "\nInner Exception: " + exception.InnerException?.ToString();
        }

        sealed class CountingValue
        {
            readonly string value;

            public CountingValue(string value)
            {
                this.value = value;
            }

            public int ToStringCalls { get; private set; }

            public override string ToString()
            {
                this.ToStringCalls++;
                return this.value;
            }
        }

        sealed class CapturedEvent
        {
            public CapturedEvent(int eventId, int payloadCount, string message, string eventType)
            {
                this.EventId = eventId;
                this.PayloadCount = payloadCount;
                this.Message = message;
                this.EventType = eventType;
            }

            public int EventId { get; }

            public int PayloadCount { get; }

            public string Message { get; }

            public string EventType { get; }
        }

        sealed class CapturingEventListener : EventListener
        {
            readonly List<CapturedEvent> events = new List<CapturedEvent>();

            public IReadOnlyList<CapturedEvent> Events => this.events;

            public void Enable(EventLevel eventLevel)
            {
                this.EnableEvents(DefaultEventSource.Log, eventLevel, DefaultEventSource.Keywords.Diagnostics);
            }

            public override void Dispose()
            {
                this.DisableEvents(DefaultEventSource.Log);
                base.Dispose();
            }

            protected override void OnEventWritten(EventWrittenEventArgs eventData)
            {
                if (eventData.EventSource == DefaultEventSource.Log &&
                    eventData.Payload != null &&
                    eventData.Payload.Count == 7 &&
                    eventData.Payload[4] is string message &&
                    eventData.Payload[6] is string eventType)
                {
                    this.events.Add(new CapturedEvent(eventData.EventId, eventData.Payload.Count, message, eventType));
                }
            }
        }
    }
}
#endif
