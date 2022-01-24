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
    using Microsoft.WindowsAzure.Storage;

    internal static class StorageUriExtensions
    {
        public static string GetAccountName(this StorageUri storageUri)
        {
            if (storageUri == null)
                throw new ArgumentNullException(nameof(storageUri));

            Uri serviceUri = storageUri.PrimaryUri;
            switch (serviceUri.HostNameType)
            {
                case UriHostNameType.Dns:
                    // Typically, Azure Storage Service URIs are formatted as <protocol>://<account>.<service>.<suffix>
                    // Note that primary and secondary endpoints must share the same resource
                    string[] segments = serviceUri.Host.Split('.');
                    if (segments.Length >= 2 && IsStorageService(segments[1]))
                        return segments[0];
                    break;
                case UriHostNameType.IPv4:
                case UriHostNameType.IPv6:
                    // In other scenarios, like for Azurite, the service URI looks like <protocol>://<host>:<port>/<account>
                    return serviceUri.GetComponents(UriComponents.Path, UriFormat.Unescaped);
            }

            // Otherwise, simply return the entire host + path
            return serviceUri.PathAndQuery == "/"
                ? serviceUri.GetComponents(UriComponents.Host, UriFormat.Unescaped)
                : serviceUri.GetComponents(UriComponents.Host | UriComponents.PathAndQuery, UriFormat.Unescaped);
        }

        private static bool IsStorageService(string segment)
            => segment == "blob" || segment == "queue" || segment == "table" || segment == "file";
    }
}
