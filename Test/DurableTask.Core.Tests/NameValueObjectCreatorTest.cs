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
    using System.Threading.Tasks;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class NameValueObjectCreatorTest
    {
        sealed class TestCreatorOrchestration : TaskOrchestration<string, string>
        {
            public override Task<string> RunTask(OrchestrationContext context, string input)
            {
                return Task.FromResult("");
            }
        }
        
        [TestMethod]
        public void CreatorWithTypeTest()
        {
            var name = "TestCreatorOrchestration";
            var version = "v1"; 
            Type type = typeof(TestCreatorOrchestration);
            var creator = new NameValueObjectCreator<TestCreatorOrchestration>(name, version, type);

            TestCreatorOrchestration instance = creator.Create();
            Assert.IsNotNull(instance);
        }
        
        [TestMethod]
        public void CreatorWithInstance()
        {
            var name = "TestCreatorOrchestration";
            var version = "v1"; 
            var creator = new NameValueObjectCreator<TestCreatorOrchestration>(name, version, new TestCreatorOrchestration());

            TestCreatorOrchestration instance = creator.Create();
            Assert.IsNotNull(instance);
        }
        
        [TestMethod]
        public void CreatorWithDelegateTest()
        {
            var name = "TestCreatorOrchestration";
            var version = "v1"; 
            var creator = new NameValueObjectCreator<TestCreatorOrchestration>(name, version, () => new TestCreatorOrchestration());

            TestCreatorOrchestration instance = creator.Create();
            Assert.IsNotNull(instance);
        }
    }
}