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

namespace DurableTask.AzureStorage.Storage
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using Microsoft.WindowsAzure.Storage;

    internal static class StorageUriExtensions
    {
        // Note that much of this class is based on internal logic from the Azure Storage SDK
        // Ports: https://github.com/Azure/azure-sdk-for-net/blob/c1c61f10b855ccdd97c790d92f26765e99fd15a8/sdk/storage/Azure.Storage.Common/src/Shared/Constants.cs#L599
        // Account Name Parse: https://github.com/Azure/azure-sdk-for-net/blob/4162f6fa2445b2127468b9cfd080f01c9da88eba/sdk/storage/Azure.Storage.Blobs/src/BlobUriBuilder.cs#L166
        // Utilities: https://github.com/Azure/azure-sdk-for-net/blob/4162f6fa2445b2127468b9cfd080f01c9da88eba/sdk/storage/Azure.Storage.Common/src/Shared/UriExtensions.cs#L16

        private static readonly HashSet<int> SasPorts = new HashSet<int> { 10000, 10001, 10002, 10003, 10004, 10100, 10101, 10102, 10103, 10104, 11000, 11001, 11002, 11003, 11004, 11100, 11101, 11102, 11103, 11104 };

        public static string GetAccountName(this StorageUri storageUri, string service)
        {
            if (storageUri == null)
                throw new ArgumentNullException(nameof(storageUri));

            if (service == null)
                throw new ArgumentNullException(nameof(service));

            // Note that the primary and secondary endpoints must share the same resource
            Uri uri = storageUri.PrimaryUri;

            if (IsHostIPEndPointStyle(uri))
            {
                // In some scenarios, like for Azurite, the service URI looks like <protocol>://<host>:<port>/<account>
                string path = GetPath(uri);
                int accountEndIndex = path.IndexOf("/", StringComparison.InvariantCulture);
                return accountEndIndex == -1 ? path : path.Substring(0, accountEndIndex);
            }

            return GetAccountNameFromDomain(uri.Host, service);
        }

        private static string GetPath(Uri uri) =>
            uri.AbsolutePath[0] == '/' ? uri.AbsolutePath.Substring(1) : uri.AbsolutePath;

        private static bool IsHostIPEndPointStyle(Uri uri) =>
            (!string.IsNullOrEmpty(uri.Host) && uri.Host.IndexOf(".", StringComparison.InvariantCulture) >= 0 && IPAddress.TryParse(uri.Host, out _))
            || SasPorts.Contains(uri.Port);

        private static string GetAccountNameFromDomain(string host, string serviceSubDomain)
        {
            // Typically, Azure Storage Service URIs are formatted as <protocol>://<account>.<service>.<suffix>
            int accountEndIndex = host.IndexOf(".", StringComparison.InvariantCulture);
            if (accountEndIndex >= 0)
            {
                int serviceStartIndex = host.IndexOf(serviceSubDomain, accountEndIndex, StringComparison.InvariantCulture);
                return serviceStartIndex > -1 ? host.Substring(0, accountEndIndex) : null;
            }

            return null;
        }
    }
}
