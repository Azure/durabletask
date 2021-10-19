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
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob.Protocol;

    [Serializable]
    class DurableTaskStorageException : Exception
    {
        public DurableTaskStorageException()
        {
        }

        public DurableTaskStorageException(string message)
            : base(message)
        {
        }

        public DurableTaskStorageException(string message, Exception inner)
            : base(message, inner)
        {
        }

        public DurableTaskStorageException(StorageException storageException)
            : base(storageException.Message, storageException)
        {
            this.HttpStatusCode = storageException.RequestInformation.HttpStatusCode;
            StorageExtendedErrorInformation extendedErrorInfo = storageException.RequestInformation.ExtendedErrorInformation;
            if (extendedErrorInfo?.ErrorCode == BlobErrorCodeStrings.LeaseLost)
            {
                LeaseLost = true;
            }
        }

        public int HttpStatusCode { get; }

        public bool LeaseLost { get; }
    }
}
