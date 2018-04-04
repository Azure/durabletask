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
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;


    /// <summary>
    /// A utility client to access the Azure blob/table storage for MessageData.
    /// </summary>
    public sealed class StorageUtil
    {
        private static volatile StorageUtil instance = null;
        private static object syncRoot = new object();
        private static string storageConnectionString;
        private static CloudStorageAccount cloudStorageAccount;
        private static CloudBlobClient cloudBlobClient;
        private static bool isInitialized;
        private const string DefaultContainerName = "durable-compressedmessage-container";

        private StorageUtil() { }

        /// <summary>
        /// The storage client instance.
        /// </summary>
        public static StorageUtil Instance
        {
            get
            {
                if (instance == null)
                {
                    lock (syncRoot)
                    {
                        if (instance == null)
                        {
                            instance = new StorageUtil();
                        }
                    }
                }

                return instance;
            }
        }

        /// <summary>
        /// Checks if the storage client has been initialized.
        /// </summary>
        public bool IsInitialized()
        {
            return isInitialized;
        }

        /// <summary>
        /// Initializes the storage client.
        /// </summary>
        public void Initialize(string connectionString)
        {
            storageConnectionString = connectionString;
            if (string.IsNullOrEmpty(storageConnectionString))
            {
                throw new ArgumentException("Storage connection string is null or empty.");
            }

            if (!CloudStorageAccount.TryParse(storageConnectionString, out cloudStorageAccount))
            {
                throw new ArgumentException("Invalid storage account connectiong string.");
            }

            isInitialized = true;
        }

        /// <summary>
        /// Uploads MessageData as bytes[] to blob container
        /// </summary>
        public string UploadToBlob(byte[] data, int dataByteCount, string blobName)
        {
            if (cloudBlobClient == null)
            {
                cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            }

            CloudBlobContainer cloudBlobContainer =
                cloudBlobClient.GetContainerReference(DefaultContainerName);
            cloudBlobContainer.CreateIfNotExists();
            CloudBlockBlob cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference(blobName);
            cloudBlockBlob.UploadFromByteArray(data, 0, dataByteCount);
            return blobName;
        }

        /// <summary>
        /// Downloads MessageData as bytes[] 
        /// </summary>
        public byte[] DownloadBlob(string blobName)
        {
            if (cloudBlobClient == null)
            {
                cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();
            }

            CloudBlobContainer cloudBlobContainer =
                cloudBlobClient.GetContainerReference(DefaultContainerName);
            CloudBlockBlob cloudBlockBlob = cloudBlobContainer.GetBlockBlobReference(blobName);

            using (MemoryStream ms = new MemoryStream())
            {
                cloudBlockBlob.DownloadToStream(ms);
                return ms.ToArray();
            }
        }
    }
}
