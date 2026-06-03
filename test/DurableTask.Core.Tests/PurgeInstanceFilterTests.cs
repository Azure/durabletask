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
    using System.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class PurgeInstanceFilterTests
    {
        [TestMethod]
        public void Timeout_DefaultIsNull()
        {
            var filter = new PurgeInstanceFilter(DateTime.UtcNow.AddDays(-1), null, null);
            Assert.IsNull(filter.Timeout);
        }

        [TestMethod]
        public void Timeout_CanBeSet()
        {
            var filter = new PurgeInstanceFilter(DateTime.UtcNow.AddDays(-1), null, null);
            filter.Timeout = TimeSpan.FromSeconds(30);
            Assert.IsNotNull(filter.Timeout);
            Assert.AreEqual(TimeSpan.FromSeconds(30), filter.Timeout.Value);
        }

        [TestMethod]
        public void Timeout_CanBeSetToNull()
        {
            var filter = new PurgeInstanceFilter(DateTime.UtcNow.AddDays(-1), null, null);
            filter.Timeout = TimeSpan.FromSeconds(30);
            filter.Timeout = null;
            Assert.IsNull(filter.Timeout);
        }

        [TestMethod]
        public void Constructor_PreservesAllProperties()
        {
            var from = DateTime.UtcNow.AddDays(-7);
            var to = DateTime.UtcNow;
            var statuses = new List<OrchestrationStatus> { OrchestrationStatus.Completed, OrchestrationStatus.Failed };
            var filter = new PurgeInstanceFilter(from, to, statuses);
            Assert.AreEqual(from, filter.CreatedTimeFrom);
            Assert.AreEqual(to, filter.CreatedTimeTo);
            Assert.IsNotNull(filter.RuntimeStatus);
            Assert.IsNull(filter.Timeout);
        }

        [TestMethod]
        public void PurgeResult_IsComplete_True()
        {
            var result = new PurgeResult(10, isComplete: true);
            Assert.AreEqual(10, result.DeletedInstanceCount);
            Assert.IsTrue(result.IsComplete.HasValue);
            Assert.IsTrue(result.IsComplete.Value);
        }

        [TestMethod]
        public void PurgeResult_IsComplete_False()
        {
            var result = new PurgeResult(5, isComplete: false);
            Assert.AreEqual(5, result.DeletedInstanceCount);
            Assert.IsTrue(result.IsComplete.HasValue);
            Assert.IsFalse(result.IsComplete.Value);
        }

        [TestMethod]
        public void PurgeResult_IsComplete_Null()
        {
            var result = new PurgeResult(3, isComplete: null);
            Assert.AreEqual(3, result.DeletedInstanceCount);
            Assert.IsNull(result.IsComplete);
        }

        [TestMethod]
        public void PurgeResult_OldConstructor_IsCompleteNull()
        {
            var result = new PurgeResult(7);
            Assert.AreEqual(7, result.DeletedInstanceCount);
            Assert.IsNull(result.IsComplete);
        }
    }
}