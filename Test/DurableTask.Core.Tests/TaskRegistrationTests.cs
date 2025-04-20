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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Xml;
    using DurableTask.Core.Command;
    using DurableTask.Core.History;
    using DurableTask.Emulator;
    using DurableTask.Test.Orchestrations;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class TaskRegistrationTests
    {
        public TaskHubWorker worker { get; private set; }

        [TestInitialize]
        public void Initialize()
        {
            var service = new LocalOrchestrationService();
            this.worker = new TaskHubWorker(service);
        }

        [TestMethod]
        [DataRow(typeof(TaskOrchInValid))]
        [DataRow(typeof(TaskOrchInValidGeneric<CancellationToken>))]
        [DataRow(typeof(TaskOrchInValidGeneric<string>))]
        [DataRow(typeof(TaskOrchGeneric<CancellationToken, string>))]
        [DataRow(typeof(TaskOrchGeneric<int, CancellationToken>))]
        [DataRow(typeof(TaskOrchGeneric<CancellationToken, CancellationToken>))]
        public void Test_RegistrationOfInValidTaskOrchestration(Type type)
        {
            Action action = () =>
            {
                this.worker.AddTaskOrchestrations(new[] { type });
            };

            Assert.ThrowsException<InvalidOperationException>(action);
        }

        [TestMethod]
        [DataRow(typeof(TaskOrchValid))]
        [DataRow(typeof(TaskOrchValidGeneric<CancellationToken>))]
        [DataRow(typeof(TaskOrchValidGeneric<string>))]
        [DataRow(typeof(TaskOrchGeneric<string, string>))]
        [DataRow(typeof(TaskOrchGeneric<int, bool>))]
        public void Test_RegistrationOfValidTaskOrchestration(Type type)
        {
            this.worker.AddTaskOrchestrations(new[] { type });
        }

        [TestMethod]
        [DataRow(typeof(SampleTaskActivity<CancellationToken, CancellationToken, CancellationToken>))]
        [DataRow(typeof(SampleTaskActivity<string, CancellationToken, string>))]
        [DataRow(typeof(SampleTaskActivity<CancellationToken, string, int>))]
        public void Test_RegistrationOfInValidTaskActivities(Type type)
        {
            Action action = () =>
            {
                this.worker.AddTaskActivities(new[] { type });
            };

            Assert.ThrowsException<InvalidOperationException>(action);
        }

        [TestMethod]
        [DataRow(typeof(SampleTaskActivity<string, int, CancellationToken>))]
        [DataRow(typeof(SampleTaskActivity<string, double, string>))]
        [DataRow(typeof(SampleTaskActivity<string, string, int>))]
        public void Test_RegistrationOfValidTaskActivities(Type type)
        {
            this.worker.AddTaskActivities(new[] { type });
        }

        public class TaskOrchValid : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                throw new NotImplementedException();
            }
        }

        public class TaskOrchValidGeneric<T> : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                throw new NotImplementedException();
            }
        }

        public class TaskOrchInValid : TaskOrchestration<string, CancellationToken>
        {
            public override Task<string> RunTask(OrchestrationContext context, CancellationToken input)
            {
                throw new NotImplementedException();
            }
        }

        public class TaskOrchInValidGeneric<T> : TaskOrchestration<string, CancellationToken>
        {
            public override Task<string> RunTask(OrchestrationContext context, CancellationToken input)
            {
                throw new NotImplementedException();
            }
        }

        public class TaskOrchGeneric<TOut, Tin> : TaskOrchestration<TOut, Tin>
        {
            public override Task<TOut> RunTask(OrchestrationContext context, Tin input)
            {
                throw new NotImplementedException();
            }
        }

        public class SampleTaskActivity<Tin, Tout, T> : TaskActivity<Tin, Tout>
        {
            protected override Tout Execute(TaskContext context, Tin input)
            {
                throw new NotImplementedException();
            }
        }
    }
}
