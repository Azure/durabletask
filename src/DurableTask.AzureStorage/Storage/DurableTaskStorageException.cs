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
namespace DurableTask.AzureStorage.Storage
{
    using System;
    using Azure;
    using Azure.Storage.Blobs.Models;
    using Azure.Storage.Queues.Models;

    [Serializable]
    class DurableTaskStorageException : Exception
    {
        public DurableTaskStorageException()
        {
        }

        public DurableTaskStorageException(string? message)
            : base(message)
        {
        }

        public DurableTaskStorageException(string? message, Exception? inner)
            : base(message, inner)
        {
        }

        public DurableTaskStorageException(RequestFailedException? requestFailedException)
            : base("An error occurred while communicating with Azure Storage", requestFailedException)
        {
            if (requestFailedException != null)
            {
                this.HttpStatusCode = requestFailedException.Status;
                this.ErrorCode = requestFailedException.ErrorCode;
                this.LeaseLost = requestFailedException?.ErrorCode == BlobErrorCode.LeaseLost;
                this.IsPopReceiptMismatch = requestFailedException?.ErrorCode == QueueErrorCode.PopReceiptMismatch;
            }
        }

        public int HttpStatusCode { get; }

        public string? ErrorCode { get; }

        public bool LeaseLost { get; }

        public bool IsPopReceiptMismatch { get; }
    }
}
