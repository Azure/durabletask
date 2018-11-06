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

namespace DurableTask.AzureStorage.Tests.Correlation
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Settings;
    using Microsoft.ApplicationInsights.Channel;
    using Microsoft.ApplicationInsights.DataContracts;
    using Microsoft.ApplicationInsights.Extensibility.Implementation;
    using Microsoft.ApplicationInsights.W3C;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;

    [TestClass]
    public class CorrelationScenarioTest
    {
        [DataTestMethod]
        [DataRow(Protocol.HttpCorrelationProtocol)]
        [DataRow(Protocol.W3CTraceContext)]
        public async Task SingleOrchestratorWithSingleActivityAsync(Protocol protocol)
        {
            CorrelationSettings.Current.Protocol = protocol;
            CorrelationSettings.Current.EnableDistributedTracing = true;
            var host = new TestCorrelationOrchestrationHost();
            List<OperationTelemetry> actual = await host.ExecuteOrchestrationAsync(typeof(SayHelloOrchestrator), "world", 360);
            Assert.AreEqual(5, actual.Count);

            CollectionAssert.AreEqual(
                new (Type, string)[]
                {
                    (typeof(RequestTelemetry), TraceConstants.Client),
                    (typeof(DependencyTelemetry), TraceConstants.Client),
                    (typeof(RequestTelemetry), $"{TraceConstants.Orchestrator} SayHelloOrchestrator"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(Hello).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Activity} Hello")
                }, actual.Select(x => (x.GetType(), x.Name)).ToList());
        }

        [KnownType(typeof(Hello))]
        internal class SayHelloOrchestrator : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                return context.ScheduleTask<string>(typeof(Hello), input);
            }
        }

        internal class Hello : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                if (string.IsNullOrEmpty(input))
                {
                    throw new ArgumentNullException(nameof(input));
                }

                Console.WriteLine($"Activity: Hello {input}");
                return $"Hello, {input}!";
            }
        }

        [DataTestMethod]
        [DataRow(Protocol.HttpCorrelationProtocol)]
        [DataRow(Protocol.W3CTraceContext)]
        public async Task SingleOrchestrationWithThrowingExceptionAsync(Protocol protocol)
        {
            CorrelationSettings.Current.Protocol = protocol;
            CorrelationSettings.Current.EnableDistributedTracing = true;
            var host = new TestCorrelationOrchestrationHost();
            // parameter = null cause an exception. 
            Tuple<List<OperationTelemetry>, List<ExceptionTelemetry>> result = await host.ExecuteOrchestrationWithExceptionAsync(typeof(SayHelloOrchestrator), null, 50);

            List<OperationTelemetry> actual = result.Item1;
            List<ExceptionTelemetry> actualExceptions = result.Item2;

            Assert.AreEqual(5, actual.Count);

            CollectionAssert.AreEqual(
                new (Type, string)[]
                {
                    (typeof(RequestTelemetry), TraceConstants.Client),
                    (typeof(DependencyTelemetry), TraceConstants.Client),
                    (typeof(RequestTelemetry), $"{TraceConstants.Orchestrator} SayHelloOrchestrator"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(Hello).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Activity} Hello")
                }, actual.Select(x => (x.GetType(), x.Name)).ToList());

            CollectionAssert.AreEqual(
                actualExceptions.Select(x => 
                     x.Context.Operation.ParentId).ToList(),
                new string[] { actual[4].Id, actual[2].Id }
                );
        }

        [DataTestMethod]
        [DataRow(Protocol.HttpCorrelationProtocol)]
        [DataRow(Protocol.W3CTraceContext)]
        public async Task SingleOrchestratorWithMultipleActivitiesAsync(Protocol protocol)
        {
            CorrelationSettings.Current.Protocol = protocol;
            CorrelationSettings.Current.EnableDistributedTracing = true;
            var host = new TestCorrelationOrchestrationHost();
            List<OperationTelemetry> actual = await host.ExecuteOrchestrationAsync(typeof(SayHelloActivities), "world", 50);
            Assert.AreEqual(7, actual.Count);

            CollectionAssert.AreEqual(
                new (Type, string)[]
                {
                    (typeof(RequestTelemetry), TraceConstants.Client),
                    (typeof(DependencyTelemetry), TraceConstants.Client),
                    (typeof(RequestTelemetry), $"{TraceConstants.Orchestrator} SayHelloActivities"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(HelloWait).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Activity} HelloWait"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(HelloWait).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Activity} HelloWait")
                }, actual.Select(x => (x.GetType(), x.Name)).ToList());
         }

        [KnownType(typeof(HelloWait))]
        internal class SayHelloActivities : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                var tasks = new List<Task<string>>();
                tasks.Add(context.ScheduleTask<string>(typeof(HelloWait), input));
                tasks.Add(context.ScheduleTask<string>(typeof(HelloWait), input));
                await Task.WhenAll(tasks);
                return $"{tasks[0].Result}:{tasks[1].Result}";
            }
        }

        internal class HelloWait : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                throw new NotImplementedException();
            }

            protected override async Task<string> ExecuteAsync(TaskContext context, string input)
            {
                if (string.IsNullOrEmpty(input))
                {
                    throw new ArgumentNullException(nameof(input));
                }

                await Task.Delay(TimeSpan.FromSeconds(2));

                Console.WriteLine($"Activity: HelloWait {input}");
                return $"Hello, {input}! I wait for 1 sec.";
            }
        }

        [DataTestMethod]
        [DataRow(Protocol.HttpCorrelationProtocol)]
        [DataRow(Protocol.W3CTraceContext)]
        public async Task SubOrchestratorAsync(Protocol protocol)
        {
            CorrelationSettings.Current.Protocol = protocol;
            CorrelationSettings.Current.EnableDistributedTracing = true;
            var host = new TestCorrelationOrchestrationHost();
            List<OperationTelemetry> actual = await host.ExecuteOrchestrationAsync(typeof(ParentOrchestrator), "world", 50);
            Assert.AreEqual(7, actual.Count);
            CollectionAssert.AreEqual(
                new (Type, string)[]
                {
                    (typeof(RequestTelemetry), TraceConstants.Client),
                    (typeof(DependencyTelemetry), TraceConstants.Client),
                    (typeof(RequestTelemetry), $"{TraceConstants.Orchestrator} ParentOrchestrator"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(ChildOrchestrator).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Orchestrator} ChildOrchestrator"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(Hello).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Activity} Hello")
                }, actual.Select(x => (x.GetType(), x.Name)).ToList());
        }

        [KnownType(typeof(ChildOrchestrator))]
        [KnownType(typeof(Hello))]
        internal class ParentOrchestrator : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                return context.CreateSubOrchestrationInstance<string>(typeof(ChildOrchestrator), input);
            }
        }

        [KnownType(typeof(Hello))]
        internal class ChildOrchestrator : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                return context.ScheduleTask<string>(typeof(Hello), input);
            }
        }

        [DataTestMethod]
        [DataRow(Protocol.HttpCorrelationProtocol)]
        [DataRow(Protocol.W3CTraceContext)]
        public async Task MultipleSubOrchestratorAsync(Protocol protocol)
        {
            CorrelationSettings.Current.Protocol = protocol;
            CorrelationSettings.Current.EnableDistributedTracing = true;
            var host = new TestCorrelationOrchestrationHost();
            List<OperationTelemetry> actual = await host.ExecuteOrchestrationAsync(typeof(ParentOrchestratorWithMultiLayeredSubOrchestrator), "world", 50);
            Assert.AreEqual(13, actual.Count);

            CollectionAssert.AreEqual(
                new (Type, string)[]
                {
                    (typeof(RequestTelemetry), TraceConstants.Client),
                    (typeof(DependencyTelemetry), TraceConstants.Client),
                    (typeof(RequestTelemetry), $"{TraceConstants.Orchestrator} ParentOrchestratorWithMultiLayeredSubOrchestrator"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(ChildOrchestratorWithMultiSubOrchestrator).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Orchestrator} ChildOrchestratorWithMultiSubOrchestrator"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(ChildOrchestrator).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Orchestrator} ChildOrchestrator"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(Hello).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Activity} Hello"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(ChildOrchestrator).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Orchestrator} ChildOrchestrator"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(Hello).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Activity} Hello"),
            }, actual.Select(x => (x.GetType(), x.Name)).ToList());
        }

        [KnownType(typeof(ChildOrchestratorWithMultiSubOrchestrator))]
        [KnownType(typeof(ChildOrchestrator))]
        [KnownType(typeof(Hello))]
        internal class ParentOrchestratorWithMultiLayeredSubOrchestrator : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                return context.CreateSubOrchestrationInstance<string>(typeof(ChildOrchestratorWithMultiSubOrchestrator), input);
            }
        }

        [KnownType(typeof(ChildOrchestrator))]
        [KnownType(typeof(Hello))]
        internal class ChildOrchestratorWithMultiSubOrchestrator : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                var tasks = new List<Task<string>>();
                tasks.Add(context.CreateSubOrchestrationInstance<string>(typeof(ChildOrchestrator), "foo"));
                tasks.Add(context.CreateSubOrchestrationInstance<string>(typeof(ChildOrchestrator), "bar"));
                await Task.WhenAll(tasks);
                return $"{tasks[0].Result}:{tasks[1].Result}";
            }
        }

        [DataTestMethod]
        [DataRow(Protocol.HttpCorrelationProtocol)]
        [DataRow(Protocol.W3CTraceContext)]
        public async Task SingleOrchestratorWithRetryAsync(Protocol protocol)
        {
            CorrelationSettings.Current.Protocol = protocol;
            CorrelationSettings.Current.EnableDistributedTracing = true;
            var host = new TestCorrelationOrchestrationHost();
            SingleOrchestrationWithRetry.ResetCounter();
            Tuple<List<OperationTelemetry>, List<ExceptionTelemetry>> resultTuple = await host.ExecuteOrchestrationWithExceptionAsync(typeof(SingleOrchestrationWithRetry), "world", 50);
            List<OperationTelemetry> actual = resultTuple.Item1;
            List<ExceptionTelemetry> actualExceptions = resultTuple.Item2;

            Assert.AreEqual(7, actual.Count);
            CollectionAssert.AreEqual(
                new (Type, string)[]
                {
                    (typeof(RequestTelemetry), TraceConstants.Client),
                    (typeof(DependencyTelemetry), TraceConstants.Client),
                    (typeof(RequestTelemetry), $"{TraceConstants.Orchestrator} SingleOrchestrationWithRetry"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(NeedToExecuteTwice).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Activity} NeedToExecuteTwice"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(NeedToExecuteTwice).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Activity} NeedToExecuteTwice")
                }, actual.Select(x => (x.GetType(), x.Name)).ToList());

            CollectionAssert.AreEqual(
                actualExceptions.Select(x => x.Context.Operation.ParentId).ToList(),
                new string[] { actual[4].Id });
        }

        [KnownType(typeof(NeedToExecuteTwice))]
        internal class SingleOrchestrationWithRetry : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                var retryOption = new RetryOptions(TimeSpan.FromMilliseconds(10), 2);
                return context.ScheduleWithRetry<string>(typeof(NeedToExecuteTwice), retryOption, input);
            }

            internal static void ResetCounter()
            {
                NeedToExecuteTwice.Counter = 0;
            }
        }

        internal class NeedToExecuteTwice : TaskActivity<string, string>
        {
            internal static int Counter = 0;

            protected override string Execute(TaskContext context, string input)
            {
                if (Counter == 0)
                {
                    Counter++;
                    throw new Exception("Something happens");
                }

                return $"Hello {input} with retry";
            }
        }

        [DataTestMethod]
        [DataRow(Protocol.HttpCorrelationProtocol)]
        [DataRow(Protocol.W3CTraceContext)]
        public async Task MultiLayeredOrchestrationWithRetryAsync(Protocol protocol)
        {
            CorrelationSettings.Current.Protocol = protocol;
            CorrelationSettings.Current.EnableDistributedTracing = true;
            MultiLayeredOrchestrationWithRetry.Reset();
            var host = new TestCorrelationOrchestrationHost();
            Tuple<List<OperationTelemetry>, List<ExceptionTelemetry>> resultTuple = await host.ExecuteOrchestrationWithExceptionAsync(typeof(MultiLayeredOrchestrationWithRetry), "world", 50);
            List<OperationTelemetry> actual = resultTuple.Item1;
            List<ExceptionTelemetry> actualExceptions = resultTuple.Item2;
            Assert.AreEqual(19, actual.Count);

            CollectionAssert.AreEqual(
                new (Type, string)[]
                {
                    (typeof(RequestTelemetry), TraceConstants.Client),
                    (typeof(DependencyTelemetry), TraceConstants.Client),
                    (typeof(RequestTelemetry), $"{TraceConstants.Orchestrator} MultiLayeredOrchestrationWithRetry"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(MultiLayeredOrchestrationChildWithRetry).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Orchestrator} MultiLayeredOrchestrationChildWithRetry"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(NeedToExecuteTwice01).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Activity} NeedToExecuteTwice01"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(MultiLayeredOrchestrationChildWithRetry).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Orchestrator} MultiLayeredOrchestrationChildWithRetry"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(NeedToExecuteTwice01).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Activity} NeedToExecuteTwice01"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(NeedToExecuteTwice02).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Activity} NeedToExecuteTwice02"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(MultiLayeredOrchestrationChildWithRetry).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Orchestrator} MultiLayeredOrchestrationChildWithRetry"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(NeedToExecuteTwice01).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Activity} NeedToExecuteTwice01"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(NeedToExecuteTwice02).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Activity} NeedToExecuteTwice02"),
                }, actual.Select(x => (x.GetType(), x.Name)).ToList());

            CollectionAssert.AreEqual(
                actualExceptions.Select(x => x.Context.Operation.ParentId).ToList(),
                new string[] { actual[6].Id , actual[4].Id, actual[12].Id, actual[8].Id});
        }

        [KnownType(typeof(MultiLayeredOrchestrationChildWithRetry))]
        [KnownType(typeof(NeedToExecuteTwice01))]
        [KnownType(typeof(NeedToExecuteTwice02))]
        internal class MultiLayeredOrchestrationWithRetry : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                var retryOption = new RetryOptions(TimeSpan.FromMilliseconds(10), 3);
                return context.CreateSubOrchestrationInstanceWithRetry<string>(typeof(MultiLayeredOrchestrationChildWithRetry), retryOption, input);
            }

            internal static void Reset()
            {
                NeedToExecuteTwice01.Counter = 0;
                NeedToExecuteTwice02.Counter = 0;
            }
        }

        [KnownType(typeof(NeedToExecuteTwice01))]
        [KnownType(typeof(NeedToExecuteTwice02))]
        internal class MultiLayeredOrchestrationChildWithRetry : TaskOrchestration<string, string>
        {
            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                var result01 = await context.ScheduleTask<string>(typeof(NeedToExecuteTwice01), input);
                var result02 = await context.ScheduleTask<string>(typeof(NeedToExecuteTwice02), input);
                return $"{result01}:{result02}";
            }
        }

        internal class NeedToExecuteTwice01 : TaskActivity<string, string>
        {
            internal static int Counter = 0;

            protected override string Execute(TaskContext context, string input)
            {
                if (Counter == 0)
                {
                    Counter++;
                    throw new Exception("Something happens");
                }

                return $"Hello {input} with retry";
            }
        }

        internal class NeedToExecuteTwice02 : TaskActivity<string, string>
        {
            internal static int Counter = 0;

            protected override string Execute(TaskContext context, string input)
            {
                if (Counter == 0)
                {
                    Counter++;
                    throw new Exception("Something happens");
                }

                return $"Hello {input} with retry";
            }
        }

        //[TestMethod] ContinueAsNew

        [DataTestMethod]
        [DataRow(Protocol.HttpCorrelationProtocol)]
        [DataRow(Protocol.W3CTraceContext)]
        public async Task ContinueAsNewAsync(Protocol protocol)
        {
            CorrelationSettings.Current.Protocol = protocol;
            CorrelationSettings.Current.EnableDistributedTracing = true;
            ContinueAsNewOrchestration.Reset();
            var host = new TestCorrelationOrchestrationHost();
            List<OperationTelemetry> actual = await host.ExecuteOrchestrationAsync(typeof(ContinueAsNewOrchestration), "world", 50);
            Assert.AreEqual(11, actual.Count);

            CollectionAssert.AreEqual(
                new (Type, string)[]
                {
                    (typeof(RequestTelemetry), TraceConstants.Client),
                    (typeof(DependencyTelemetry), TraceConstants.Client),
                    (typeof(RequestTelemetry), $"{TraceConstants.Orchestrator} ContinueAsNewOrchestration"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(Hello).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Activity} Hello"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(Hello).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Activity} Hello"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(Hello).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Activity} Hello"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(Hello).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Activity} Hello"),
                }, actual.Select(x => (x.GetType(), x.Name)).ToList());
        }

        [KnownType(typeof(Hello))]
        internal class ContinueAsNewOrchestration : TaskOrchestration<string, string>
        {
            static int counter = 0;

            public override async Task<string> RunTask(OrchestrationContext context, string input)
            {
                string result = await context.ScheduleTask<string>(typeof(Hello), input);
                result = input + ":" + result;
                if (counter < 3)
                {
                    counter++;
                    context.ContinueAsNew(result);
                }

                return result;
            }

            internal static void Reset()
            {
                counter = 0;
            }
        }

        [DataTestMethod]
        [DataRow(Protocol.HttpCorrelationProtocol)]
        [DataRow(Protocol.W3CTraceContext)]
        public async Task MultipleParentScenarioAsync(Protocol protocol)
        {
            MultiParentOrchestrator.Reset();
            CorrelationSettings.Current.Protocol = protocol;
            CorrelationSettings.Current.EnableDistributedTracing = true;
            var host = new TestCorrelationOrchestrationHost();
            var tasks = new List<Task>();
            tasks.Add(host.ExecuteOrchestrationAsync(typeof(MultiParentOrchestrator), "world", 30));

            while (IsNotReadyForRaiseEvent(host.Client))
            {
                await Task.Delay(TimeSpan.FromMilliseconds(300));
            }

            tasks.Add(host.Client.RaiseEventAsync("someEvent", "hi"));
            await Task.WhenAll(tasks);

            List<OperationTelemetry> actual = Convert(tasks[0]);

            Assert.AreEqual(5, actual.Count);
            CollectionAssert.AreEqual(
                new (Type, string)[]
                {
                    (typeof(RequestTelemetry), TraceConstants.Client),
                    (typeof(DependencyTelemetry), TraceConstants.Client),
                    (typeof(RequestTelemetry), $"{TraceConstants.Orchestrator} MultiParentOrchestrator"),
                    (typeof(DependencyTelemetry), $"{TraceConstants.Orchestrator} {typeof(Hello).FullName}"),
                    (typeof(RequestTelemetry), $"{TraceConstants.Activity} Hello")
                }, actual.Select(x => (x.GetType(), x.Name)).ToList());
        }

        bool IsNotReadyForRaiseEvent(TestOrchestrationClient client)
        {
            return client == null && !MultiParentOrchestrator.IsWaitForExternalEvent;
        }

        List<OperationTelemetry> Convert(Task task)
        {
            return (task as Task<List<OperationTelemetry>>)?.Result;
        }

        [KnownType(typeof(Hello))]
        internal class MultiParentOrchestrator : TaskOrchestration<string, string>
        {
            public static bool IsWaitForExternalEvent { get; set; } = false;

            readonly TaskCompletionSource<object> receiveEvent = new TaskCompletionSource<object>();

            public async override Task<string> RunTask(OrchestrationContext context, string input)
            {
                IsWaitForExternalEvent = true;
                await this.receiveEvent.Task;
                await context.ScheduleTask<string>(typeof(Hello), input);
                return "done";
            }

            public override void OnEvent(OrchestrationContext context, string name, string input)
            {
                this.receiveEvent.SetResult(null);
            }

            internal static void Reset()
            {
                IsWaitForExternalEvent = false;
            }
        }

        [DataTestMethod]
        [DataRow(Protocol.HttpCorrelationProtocol)]
        [DataRow(Protocol.W3CTraceContext)]
        public async Task SuppressTelemetryAsync(Protocol protocol)
        {
            CorrelationSettings.Current.Protocol = protocol;
            CorrelationSettings.Current.EnableDistributedTracing = false;
            MultiLayeredOrchestrationWithRetry.Reset();
            var host = new TestCorrelationOrchestrationHost();
            Tuple<List<OperationTelemetry>, List<ExceptionTelemetry>> resultTuple = await host.ExecuteOrchestrationWithExceptionAsync(typeof(MultiLayeredOrchestrationWithRetry), "world", 50);
            List<OperationTelemetry> actual = resultTuple.Item1;
            List<ExceptionTelemetry> actualExceptions = resultTuple.Item2;
            Assert.AreEqual(0, actual.Count);
            Assert.AreEqual(0, actualExceptions.Count);
        }

        //[TestMethod] terminate

        class TestCorrelationOrchestrationHost
        {
            internal TestOrchestrationClient Client { get; set; }

            internal async Task<Tuple<List<OperationTelemetry>, List<ExceptionTelemetry>>> ExecuteOrchestrationWithExceptionAsync(Type orchestrationType, string parameter, int timeout)
            {
                var sendItems = new ConcurrentQueue<ITelemetry>();
                await ExtractTelemetry(orchestrationType, parameter, timeout, sendItems);

                var sendItemList = ConvertTo(sendItems);
                var operationTelemetryList = sendItemList.OfType<OperationTelemetry>();
                var exceptionTelemetryList = sendItemList.OfType<ExceptionTelemetry>().ToList();

                List<OperationTelemetry> operationTelemetries = FilterOperationTelemetry(operationTelemetryList).ToList().CorrelationSort();

                return new Tuple<List<OperationTelemetry>, List<ExceptionTelemetry>>(operationTelemetries, exceptionTelemetryList);
            }

            internal async Task<List<OperationTelemetry>> ExecuteOrchestrationAsync(Type orchestrationType, string parameter, int timeout)
            {
                var sendItems = new ConcurrentQueue<ITelemetry>();
                await ExtractTelemetry(orchestrationType, parameter, timeout, sendItems);

                var sendItemList = ConvertTo(sendItems);
                var operationTelemetryList = sendItemList.OfType<OperationTelemetry>();

                var result = FilterOperationTelemetry(operationTelemetryList).ToList();
                Debug.WriteLine(
                    JsonConvert.SerializeObject(
                        result.Select(
                            x => new
                            {
                                Type = x.GetType().Name,
                                OperationName = x.Name,
                                Id = x.Id,
                                OperationId = x.Context.Operation.Id,
                                OperationParentId = x.Context.Operation.ParentId,
                            })));

                return result.CorrelationSort();
            }

            IEnumerable<OperationTelemetry> FilterOperationTelemetry(IEnumerable<OperationTelemetry> operationTelemetries)
            {
                return operationTelemetries.Where(
                    p => p.Name.Contains(TraceConstants.Activity) || p.Name.Contains(TraceConstants.Orchestrator) || p.Name.Contains(TraceConstants.Client) || p.Name.Contains("Operation"));
            }

            async Task ExtractTelemetry(Type orchestrationType, string parameter, int timeout, ConcurrentQueue<ITelemetry> sendItems)
            {
                var sendAction = new Action<ITelemetry>(
                    delegate(ITelemetry telemetry) { sendItems.Enqueue(telemetry); });
                new TelemetryActivator().Initialize(sendAction, Guid.NewGuid().ToString());
                // new TelemetryActivator().Initialize(item => sendItems.Enqueue(item), Guid.NewGuid().ToString());
                using (TestOrchestrationHost host = TestHelpers.GetTestOrchestrationHost(false))
                {
                    await host.StartAsync();
                    var activity = new Activity(TraceConstants.Client);

                    if (CorrelationSettings.Current.Protocol == Protocol.W3CTraceContext)
                    {
#pragma warning disable 618
                        activity.GenerateW3CContext();
#pragma warning restore 618
                    }

                    activity.Start();
                    Client = await host.StartOrchestrationAsync(orchestrationType, parameter);
                    await Client.WaitForCompletionAsync(TimeSpan.FromSeconds(timeout));

                    await host.StopAsync();
                }
            }

            List<ITelemetry> ConvertTo(ConcurrentQueue<ITelemetry> queue)
            {
                var converted = new List<ITelemetry>();
                while (!queue.IsEmpty)
                {
                    ITelemetry x;
                    if (queue.TryDequeue(out x))
                    {
                        converted.Add(x);
                    }
                }

                return converted;
            }
        }
    }
}
