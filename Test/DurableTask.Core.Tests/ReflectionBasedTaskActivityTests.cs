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
    using System.Reflection;
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
    public class ReflectionBasedTaskActivityTests
    {
        public TaskHubWorker worker { get; private set; }

        [TestInitialize]
        public void Initialize()
        {
            var service = new LocalOrchestrationService();
            this.worker = new TaskHubWorker(service);
        }

        [TestMethod]
        public void Test_AddTaskActivitiesFromInterface()
        {
            IReflectionBasedTaskActivityTest reflectionBasedTaskActivityTest = new ReflectionBasedTaskActivityTest();

            worker.AddTaskActivitiesFromInterface(reflectionBasedTaskActivityTest);
            worker.AddTaskActivitiesFromInterface(reflectionBasedTaskActivityTest, useFullyQualifiedMethodNames: true);
        }

        [TestMethod]
        public void Test_ReflectionBasedTaskActivity()
        {
            IReflectionBasedTaskActivityTest reflectionBasedTaskActivityTest = new ReflectionBasedTaskActivityTest();

            worker.AddTaskActivitiesFromInterface(reflectionBasedTaskActivityTest);
            worker.AddTaskActivitiesFromInterface(reflectionBasedTaskActivityTest, useFullyQualifiedMethodNames: true);
        }

        [TestMethod]
        public void Test_ReflectionBasedTaskActivityWithGeneric()
        {
            IReflectionBasedTaskActivityWithGenericTest reflectionBasedTaskActivityTest = new ReflectionBasedTaskActivityTest();

            worker.AddTaskActivitiesFromInterface(reflectionBasedTaskActivityTest);
            worker.AddTaskActivitiesFromInterface(reflectionBasedTaskActivityTest, useFullyQualifiedMethodNames: true);
        }

        [TestMethod]
        public void Test_AddTaskActivitiesFromInterfaceWithCancellationToken()
        {
            IReflectionBasedTaskActivityWithCancellationTokenTest reflectionBasedTaskActivityTest = new ReflectionBasedTaskActivityTest();
            Action action = () =>
            {
                worker.AddTaskActivitiesFromInterface(reflectionBasedTaskActivityTest);
            };

            Assert.ThrowsException<InvalidOperationException>(action);

            action = () =>
            {
                worker.AddTaskActivitiesFromInterface(reflectionBasedTaskActivityTest, useFullyQualifiedMethodNames: true);
            };

            Assert.ThrowsException<InvalidOperationException>(action);
        }

        [TestMethod]
        public void Test_ReflectionBasedTaskActivityWithCancellationToken()
        {
            IReflectionBasedTaskActivityWithCancellationTokenTest reflectionBasedTaskActivityTest = new ReflectionBasedTaskActivityTest();
            Action action = () =>
            {
                worker.AddTaskActivitiesFromInterface(reflectionBasedTaskActivityTest);
            };

            Assert.ThrowsException<InvalidOperationException>(action);

            action = () =>
            {
                worker.AddTaskActivitiesFromInterface(reflectionBasedTaskActivityTest, useFullyQualifiedMethodNames: true);
            };

            Assert.ThrowsException<InvalidOperationException>(action);
        }

        public interface IReflectionBasedTaskActivityTest
        {
            void TestMethod1();
            void TestMethod2(int n1, int n2);
        }

        public interface IReflectionBasedTaskActivityWithGenericTest
        {
            void TestMethodG3<T>(T obj);
        }

        public interface IReflectionBasedTaskActivityWithCancellationTokenTest : IReflectionBasedTaskActivityTest
        {
            void TestMethod3(int n, CancellationToken cancellationToken);
        }

        public class ReflectionBasedTaskActivityTest : IReflectionBasedTaskActivityWithCancellationTokenTest, IReflectionBasedTaskActivityWithGenericTest
        {
            public void TestMethod1()
            {
            }

            public void TestMethod2(int n1, int n2)
            {
            }

            public void TestMethod3(int n, CancellationToken cancellationToken)
            {
            }

            public void TestMethodG3<T>(T obj)
            {
            }
        }
    }
}
