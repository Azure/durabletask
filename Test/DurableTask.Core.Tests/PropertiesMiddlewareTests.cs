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
#nullable enable
namespace DurableTask.Core.Tests
{
    using System;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using DurableTask.Emulator;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class PropertiesMiddlewareTests
    {
        private const string PropertyKey = "Test";
        private const string PropertyValue = "Value";

        TaskHubWorker worker = null!;
        TaskHubClient client = null!;

        [TestInitialize]
        public async Task Initialize()
        {
            var service = new LocalOrchestrationService();
            this.worker = new TaskHubWorker(service);

            await this.worker
                .AddTaskOrchestrations(typeof(NoActivities), typeof(RunActivityOrchestrator))
                .AddTaskActivities(typeof(ReturnPropertyActivity))
                .StartAsync();

            this.client = new TaskHubClient(service);
        }

        [TestCleanup]
        public async Task TestCleanup()
        {
            await this.worker.StopAsync(true);
        }

        private sealed class NoActivities : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                return Task.FromResult(context.GetProperty<string>(PropertyKey)!);
            }
        }

        private sealed class ReturnPropertyActivity : TaskActivity<string, string>
        {
            protected override string Execute(TaskContext context, string input)
            {
                return context.GetProperty<string>(PropertyKey)!;
            }
        }

        private sealed class RunActivityOrchestrator : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                return context.ScheduleTask<string>(typeof(ReturnPropertyActivity));
            }
        }

        [TestMethod]
        public async Task OrchestrationGetsProperties()
        {
            this.worker.AddOrchestrationDispatcherMiddleware((context, next) =>
            {
                context.SetProperty<string>(PropertyKey, PropertyValue);

                return next();
            });

            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(typeof(NoActivities), null);

            TimeSpan timeout = TimeSpan.FromSeconds(Debugger.IsAttached ? 1000 : 10);
            var state = await this.client.WaitForOrchestrationAsync(instance, timeout);

            Assert.AreEqual($"\"{PropertyValue}\"", state.Output);
        }

        [TestMethod]
        public async Task OrchestrationDoesNotGetPropertiesFromActivityMiddleware()
        {
            this.worker.AddActivityDispatcherMiddleware((context, next) =>
            {
                context.SetProperty<string>(PropertyKey, PropertyValue);

                return next();
            });

            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(typeof(NoActivities), null);

            TimeSpan timeout = TimeSpan.FromSeconds(Debugger.IsAttached ? 1000 : 10);
            var state = await this.client.WaitForOrchestrationAsync(instance, timeout);

            Assert.IsNull(state.Output);
        }

        [TestMethod]
        public async Task ActivityGetsProperties()
        {
            this.worker.AddActivityDispatcherMiddleware((context, next) =>
            {
                context.SetProperty<string>(PropertyKey, PropertyValue);

                return next();
            });

            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(typeof(RunActivityOrchestrator), null);

            TimeSpan timeout = TimeSpan.FromSeconds(Debugger.IsAttached ? 1000 : 10);
            var state = await this.client.WaitForOrchestrationAsync(instance, timeout);

            Assert.AreEqual($"\"{PropertyValue}\"", state.Output);
        }

        [TestMethod]
        public async Task ActivityDoesNotGetPropertiesFromOrchestratorMiddleware()
        {
            this.worker.AddOrchestrationDispatcherMiddleware((context, next) =>
            {
                context.SetProperty<string>(PropertyKey, PropertyValue);

                return next();
            });

            OrchestrationInstance instance = await this.client.CreateOrchestrationInstanceAsync(typeof(RunActivityOrchestrator), null);

            TimeSpan timeout = TimeSpan.FromSeconds(Debugger.IsAttached ? 1000 : 10);
            var state = await this.client.WaitForOrchestrationAsync(instance, timeout);

            Assert.IsNull(state.Output);
        }
    }
}
