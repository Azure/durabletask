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

namespace DurableTask.Core.Tests
{
    using System;
    using System.Diagnostics;
    using System.Text;
    using System.Threading.Tasks;
    using DurableTask.Core.History;
    using DurableTask.Emulator;
    using DurableTask.Test.Orchestrations;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DispatcherMiddlewareTests
    {
        TaskHubWorker worker;
        TaskHubClient client;

        [TestInitialize]
        public async Task Initialize()
        {
            var service = new LocalOrchestrationService();
            this.worker = new TaskHubWorker(service);

            await this.worker
                .AddTaskOrchestrations(typeof(SimplestGreetingsOrchestration))
                .AddTaskActivities(typeof(SimplestGetUserTask), typeof(SimplestSendGreetingTask))
                .StartAsync();

            this.client = new TaskHubClient(service);
        }

        [TestCleanup]
        public async Task TestCleanup()
        {
            await this.worker.StopAsync(true);
        }

        [TestMethod]
        public async Task DispatchMiddlewareContextBuiltInProperties()
        {
            TaskOrchestration orchestration = null;
            OrchestrationRuntimeState state = null;
            OrchestrationInstance instance1 = null;

            TaskActivity activity = null;
            TaskScheduledEvent taskScheduledEvent = null;
            OrchestrationInstance instance2 = null;

            this.worker.AddOrchestrationDispatcherMiddleware((context, next) =>
            {
                orchestration = context.GetProperty<TaskOrchestration>();
                state = context.GetProperty<OrchestrationRuntimeState>();
                instance1 = context.GetProperty<OrchestrationInstance>();

                return next();
            });

            this.worker.AddActivityDispatcherMiddleware((context, next) =>
            {
                activity = context.GetProperty<TaskActivity>();
                taskScheduledEvent = context.GetProperty<TaskScheduledEvent>();
                instance2 = context.GetProperty<OrchestrationInstance>();

                return next();
            });

            var instance = await this.client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), null);

            TimeSpan timeout = TimeSpan.FromSeconds(Debugger.IsAttached ? 1000 : 10);
            await this.client.WaitForOrchestrationAsync(instance, timeout);

            Assert.IsNotNull(orchestration);
            Assert.IsNotNull(state);
            Assert.IsNotNull(instance1);

            Assert.IsNotNull(activity);
            Assert.IsNotNull(taskScheduledEvent);
            Assert.IsNotNull(instance2);

            Assert.AreNotSame(instance1, instance2);
            Assert.AreEqual(instance1.InstanceId, instance2.InstanceId);
        }

        [TestMethod]
        public async Task OrchestrationDispatcherMiddlewareContextFlow()
        {
            StringBuilder output = null;

            for (int i = 0; i < 10; i++)
            {
                string value = i.ToString();
                this.worker.AddOrchestrationDispatcherMiddleware(async (context, next) =>
                {
                    output = context.GetProperty<StringBuilder>("output");
                    if (output == null)
                    {
                        output = new StringBuilder();
                        context.SetProperty("output", output);
                    }

                    output.Append(value);
                    await next();
                    output.Append(value);
                });
            }
            
            var instance = await this.client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), null);

            TimeSpan timeout = TimeSpan.FromSeconds(Debugger.IsAttached ? 1000 : 10);
            await this.client.WaitForOrchestrationAsync(instance, timeout);

            // Each reply gets a new context, so the output should stay the same regardless of how
            // many replays an orchestration goes through.
            Assert.IsNotNull(output);
            Assert.AreEqual("01234567899876543210", output.ToString());
        }

        [TestMethod]
        public async Task ActivityDispatcherMiddlewareContextFlow()
        {
            StringBuilder output = null;

            for (int i = 0; i < 10; i++)
            {
                string value = i.ToString();
                this.worker.AddActivityDispatcherMiddleware(async (context, next) =>
                {
                    output = context.GetProperty<StringBuilder>("output");
                    if (output == null)
                    {
                        output = new StringBuilder();
                        context.SetProperty("output", output);
                    }

                    output.Append(value);
                    await next();
                    output.Append(value);
                });
            }

            var instance = await this.client.CreateOrchestrationInstanceAsync(typeof(SimplestGreetingsOrchestration), null);

            TimeSpan timeout = TimeSpan.FromSeconds(Debugger.IsAttached ? 1000 : 10);
            await this.client.WaitForOrchestrationAsync(instance, timeout);

            // Each actiivty gets a new context, so the output should stay the same regardless of how
            // many activities an orchestration schedules (as long as there is at least one).
            Assert.IsNotNull(output);
            Assert.AreEqual("01234567899876543210", output.ToString());
        }
    }
}
