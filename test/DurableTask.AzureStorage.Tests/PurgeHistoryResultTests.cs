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
    using DurableTask.Core;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class PurgeHistoryResultTests
    {
        [TestMethod]
        public void Constructor_WithoutIsComplete_IsCompleteIsNull()
        {
            // Arrange & Act
            var result = new PurgeHistoryResult(storageRequests: 5, instancesDeleted: 3, rowsDeleted: 10);

            // Assert
            Assert.AreEqual(5, result.StorageRequests);
            Assert.AreEqual(3, result.InstancesDeleted);
            Assert.AreEqual(10, result.RowsDeleted);
            Assert.IsNull(result.IsComplete);
        }

        [TestMethod]
        public void Constructor_WithIsCompleteTrue_SetsProperty()
        {
            // Arrange & Act
            var result = new PurgeHistoryResult(storageRequests: 10, instancesDeleted: 5, rowsDeleted: 20, isComplete: true);

            // Assert
            Assert.AreEqual(10, result.StorageRequests);
            Assert.AreEqual(5, result.InstancesDeleted);
            Assert.AreEqual(20, result.RowsDeleted);
            Assert.IsTrue(result.IsComplete.HasValue);
            Assert.IsTrue(result.IsComplete.Value);
        }

        [TestMethod]
        public void Constructor_WithIsCompleteFalse_SetsProperty()
        {
            // Arrange & Act
            var result = new PurgeHistoryResult(storageRequests: 8, instancesDeleted: 2, rowsDeleted: 15, isComplete: false);

            // Assert
            Assert.IsTrue(result.IsComplete.HasValue);
            Assert.IsFalse(result.IsComplete.Value);
        }

        [TestMethod]
        public void Constructor_WithIsCompleteNull_SetsProperty()
        {
            // Arrange & Act
            var result = new PurgeHistoryResult(storageRequests: 3, instancesDeleted: 1, rowsDeleted: 5, isComplete: null);

            // Assert
            Assert.IsNull(result.IsComplete);
        }

        [TestMethod]
        public void ToCorePurgeHistoryResult_IsCompleteTrue_PropagatedToPurgeResult()
        {
            // Arrange
            var result = new PurgeHistoryResult(storageRequests: 10, instancesDeleted: 5, rowsDeleted: 20, isComplete: true);

            // Act
            PurgeResult coreResult = result.ToCorePurgeHistoryResult();

            // Assert
            Assert.AreEqual(5, coreResult.DeletedInstanceCount);
            Assert.IsTrue(coreResult.IsComplete.HasValue);
            Assert.IsTrue(coreResult.IsComplete.Value);
        }

        [TestMethod]
        public void ToCorePurgeHistoryResult_IsCompleteFalse_PropagatedToPurgeResult()
        {
            // Arrange
            var result = new PurgeHistoryResult(storageRequests: 8, instancesDeleted: 2, rowsDeleted: 15, isComplete: false);

            // Act
            PurgeResult coreResult = result.ToCorePurgeHistoryResult();

            // Assert
            Assert.AreEqual(2, coreResult.DeletedInstanceCount);
            Assert.IsTrue(coreResult.IsComplete.HasValue);
            Assert.IsFalse(coreResult.IsComplete.Value);
        }

        [TestMethod]
        public void ToCorePurgeHistoryResult_IsCompleteNull_PropagatedToPurgeResult()
        {
            // Arrange
            var result = new PurgeHistoryResult(storageRequests: 3, instancesDeleted: 1, rowsDeleted: 5, isComplete: null);

            // Act
            PurgeResult coreResult = result.ToCorePurgeHistoryResult();

            // Assert
            Assert.AreEqual(1, coreResult.DeletedInstanceCount);
            Assert.IsNull(coreResult.IsComplete);
        }

        [TestMethod]
        public void ToCorePurgeHistoryResult_OldConstructor_IsCompleteNull()
        {
            // Arrange - using the old constructor without IsComplete (backward compat)
            var result = new PurgeHistoryResult(storageRequests: 5, instancesDeleted: 3, rowsDeleted: 10);

            // Act
            PurgeResult coreResult = result.ToCorePurgeHistoryResult();

            // Assert
            Assert.AreEqual(3, coreResult.DeletedInstanceCount);
            Assert.IsNull(coreResult.IsComplete);
        }
    }
}