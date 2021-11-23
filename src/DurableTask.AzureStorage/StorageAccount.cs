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

using System;
using Microsoft.WindowsAzure.Storage;

namespace DurableTask.AzureStorage
{
    /// <summary>
    /// Provides a set of <see langword="static"/> methods concerning Azure Storage Accounts.
    /// </summary>
    public static class StorageAccount
    {
        private const string DefaultEndpointSuffix = "core.windows.net";
        private const string DefaultScheme = "https";

        // The usage of the secondary suffix is documented here:
        // https://docs.microsoft.com/en-us/azure/storage/common/storage-redundancy#design-your-applications-for-read-access-to-the-secondary
        private const string DefaultSecondarySuffix = "-secondary";

        /// <summary>
        /// Gets the default storage service endpoint for the specified <paramref name="accountName"/>.
        /// </summary>
        /// <param name="accountName">The Azure Storage account name.</param>
        /// <param name="service">The storage service for the desired endpoint.</param>
        /// <param name="location">
        /// Optionally indicates the whether to specify the primary or secondary endpoint. By default, the
        /// <see cref="StorageLocation.Primary"/> is used.
        /// </param>
        /// <returns>A <see cref="Uri"/> whose value corresponds with the desired default service endpoint.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="accountName"/> is <see langword="null"/> or empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="service"/> is not defined.</exception>
        public static Uri GetDefaultServiceUri(string accountName, StorageServiceType service, StorageLocation location = StorageLocation.Primary)
        {
            if (string.IsNullOrEmpty(accountName))
            {
                throw new ArgumentException("A non-empty account name must be specified.", nameof(accountName));
            }

            if (!Enum.IsDefined(typeof(StorageServiceType), service))
            {
                throw new ArgumentOutOfRangeException(nameof(service), $"Unknown storage service '{service}'.");
            }

            string suffix = location switch
            {
                StorageLocation.Primary => string.Empty,
                StorageLocation.Secondary => DefaultSecondarySuffix,
                _ => throw new ArgumentOutOfRangeException(nameof(location), $"Unknown storage location '{location}'."),
            };

            return new Uri(
                $"{DefaultScheme}://{accountName + suffix}.{service.ToString("G").ToLowerInvariant()}.{DefaultEndpointSuffix}",
                UriKind.Absolute);
        }
    }
}
