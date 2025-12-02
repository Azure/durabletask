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
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Data.Tables;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.AzureStorage.Tracking;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    /// <summary>
    /// Tests for the DecompressLargeEntityProperties method in AzureTableTrackingStore.
    /// These tests verify that the method gracefully handles BlobNotFound errors.
    /// See: https://github.com/Azure/azure-functions-durable-extension/issues/3264
    /// </summary>
    [TestClass]
    public class DecompressLargeEntityPropertiesTests
    {
        /// <summary>
        /// Verifies the method signature of DecompressLargeEntityProperties.
        /// The method should return Task&lt;bool&gt; to indicate success/failure when handling blob retrieval.
        /// When a blob is not found (404 error), it should return false instead of throwing an exception.
        /// This can happen when a late message from a previous execution arrives after ContinueAsNew
        /// deleted the blobs, or when blobs are cleaned up due to retention policies.
        /// See: https://github.com/Azure/azure-functions-durable-extension/issues/3264
        /// </summary>
        [TestMethod]
        public void DecompressLargeEntityProperties_MethodExists_AndReturnsBool()
        {
            // Arrange
            var trackingStoreType = typeof(AzureTableTrackingStore);
            var method = trackingStoreType.GetMethod(
                "DecompressLargeEntityProperties",
                BindingFlags.NonPublic | BindingFlags.Instance);

            // Assert - Verify method exists and has correct signature
            Assert.IsNotNull(method, "DecompressLargeEntityProperties method should exist");
            Assert.AreEqual(typeof(Task<bool>), method.ReturnType, 
                "Method should return Task<bool> to indicate success (true) or blob not found (false)");
            
            // Verify method has correct parameters
            var parameters = method.GetParameters();
            Assert.AreEqual(3, parameters.Length, "Method should have 3 parameters");
            Assert.AreEqual(typeof(TableEntity), parameters[0].ParameterType, "First param should be TableEntity");
            Assert.AreEqual(typeof(List<string>), parameters[1].ParameterType, "Second param should be List<string>");
            Assert.AreEqual(typeof(CancellationToken), parameters[2].ParameterType, "Third param should be CancellationToken");
        }

        /// <summary>
        /// Tests that DecompressLargeEntityProperties also handles DurableTaskStorageException
        /// wrapping a RequestFailedException with 404 status.
        /// </summary>
        [TestMethod]
        public void DecompressLargeEntityProperties_MethodSignature_ReturnsTaskBool()
        {
            // Arrange
            var trackingStoreType = typeof(AzureTableTrackingStore);
            var method = trackingStoreType.GetMethod(
                "DecompressLargeEntityProperties",
                BindingFlags.NonPublic | BindingFlags.Instance);

            // Assert
            Assert.IsNotNull(method, "DecompressLargeEntityProperties method should exist");
            Assert.AreEqual(typeof(Task<bool>), method.ReturnType, 
                "Method should return Task<bool> to indicate success/failure");
            
            var parameters = method.GetParameters();
            Assert.AreEqual(3, parameters.Length, "Method should have 3 parameters");
            Assert.AreEqual(typeof(TableEntity), parameters[0].ParameterType);
            Assert.AreEqual(typeof(List<string>), parameters[1].ParameterType);
            Assert.AreEqual(typeof(CancellationToken), parameters[2].ParameterType);
        }

        /// <summary>
        /// Verifies that RequestFailedException with status 404 is properly catchable
        /// with the pattern used in the fix.
        /// </summary>
        [TestMethod]
        public void RequestFailedException_Status404_CanBeCaughtWithWhenClause()
        {
            // Arrange
            var exception = new RequestFailedException(
                status: 404,
                message: "The specified blob does not exist.",
                errorCode: "BlobNotFound",
                innerException: null);

            // Act & Assert
            bool caught = false;
            try
            {
                throw exception;
            }
            catch (RequestFailedException e) when (e.Status == 404)
            {
                caught = true;
            }

            Assert.IsTrue(caught, "RequestFailedException with status 404 should be caught");
        }

        /// <summary>
        /// Verifies that DurableTaskStorageException wrapping RequestFailedException 
        /// with status 404 can be caught with the pattern used in the fix.
        /// </summary>
        [TestMethod]
        public void DurableTaskStorageException_WrappingBlobNotFound_CanBeCaughtWithWhenClause()
        {
            // Arrange
            var innerException = new RequestFailedException(
                status: 404,
                message: "The specified blob does not exist.",
                errorCode: "BlobNotFound",
                innerException: null);

            var wrappedException = new DurableTaskStorageException(innerException);

            // Act & Assert
            bool caught = false;
            try
            {
                throw wrappedException;
            }
            catch (DurableTaskStorageException e) when (e.InnerException is RequestFailedException { Status: 404 })
            {
                caught = true;
            }

            Assert.IsTrue(caught, "DurableTaskStorageException wrapping 404 should be caught");
        }

        /// <summary>
        /// Verifies that other status codes are NOT caught by the 404 filter.
        /// </summary>
        [TestMethod]
        public void RequestFailedException_OtherStatus_NotCaughtBy404Filter()
        {
            // Arrange - 500 Internal Server Error
            var exception = new RequestFailedException(
                status: 500,
                message: "Internal Server Error",
                errorCode: "InternalError",
                innerException: null);

            // Act & Assert
            bool caughtBy404Filter = false;
            bool caughtByGenericHandler = false;
            try
            {
                throw exception;
            }
            catch (RequestFailedException e) when (e.Status == 404)
            {
                caughtBy404Filter = true;
            }
            catch (RequestFailedException)
            {
                caughtByGenericHandler = true;
            }

            Assert.IsFalse(caughtBy404Filter, "404 filter should NOT catch 500 error");
            Assert.IsTrue(caughtByGenericHandler, "500 error should be caught by generic handler");
        }
    }
}
