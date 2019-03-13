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

namespace DurableTask.AzureStorage
{
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;

    /// <summary>
    /// Connection details of the Azure Storage account
    /// </summary>
    public sealed class StorageAccountDetails
    {
        /// <summary>
        /// The storage account credentials
        /// </summary>
        public StorageCredentials StorageCredentials { get; set; }

        /// <summary>
        /// The storage account name
        /// </summary>
        public string AccountName { get; set; }

        /// <summary>
        /// The storage account endpoint suffix
        /// </summary>
        public string EndpointSuffix { get; set; }

        /// <summary>
        /// The storage account connection string.
        /// </summary>
        /// <remarks>
        /// If specified, this value overrides any other settings.
        /// </remarks>
        public string ConnectionString { get; set; }

        /// <summary>
        ///  Convert this to its equivalent CloudStorageAccount.
        /// </summary>
        public CloudStorageAccount ToCloudStorageAccount()
        {
            if (!string.IsNullOrEmpty(this.ConnectionString))
            {
                return CloudStorageAccount.Parse(this.ConnectionString);
            }
            else
            {
                return new CloudStorageAccount(
                    this.StorageCredentials,
                    this.AccountName,
                    this.EndpointSuffix,
                    useHttps: true);
            }
        }
    }
}
