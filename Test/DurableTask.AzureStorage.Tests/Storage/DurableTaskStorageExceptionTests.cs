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
namespace DurableTask.AzureStorage.Tests.Storage
{
    using System.Net;
    using Azure;
    using Azure.Storage.Blobs.Models;
    using DurableTask.AzureStorage.Storage;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class DurableTaskStorageExceptionTests
    {
        [TestMethod]
        public void MissingRequestFailedException()
        {
            DurableTaskStorageException exception = new((RequestFailedException)null!);
            Assert.AreEqual(0, exception.HttpStatusCode);
            Assert.IsFalse(exception.LeaseLost);
        }

        [DataTestMethod]
        [DataRow(true, HttpStatusCode.Conflict, nameof(BlobErrorCode.LeaseLost))]
        [DataRow(false, HttpStatusCode.Conflict, nameof(BlobErrorCode.LeaseNotPresentWithBlobOperation))]
        [DataRow(false, HttpStatusCode.NotFound, nameof(BlobErrorCode.BlobNotFound))]
        public void ValidRequestFailedException(bool expectedLease, HttpStatusCode statusCode, string errorCode)
        {
            DurableTaskStorageException exception = new(new RequestFailedException((int)statusCode, "Error!", errorCode, innerException: null));
            Assert.AreEqual((int)statusCode, exception.HttpStatusCode);
            Assert.AreEqual(expectedLease, exception.LeaseLost);
        }
    }
}
