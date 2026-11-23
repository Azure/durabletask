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
namespace DurableTask.AzureStorage
{
    using Azure.Core;

    /// <summary>
    /// Optional capability of a storage client provider used to obtain Table user delegation keys.
    /// </summary>
    /// <remarks>
    /// Entra-authenticated Table providers must expose the same credential they use to create the service client.
    /// Other providers do not need to implement this interface unless they support Entra authentication.
    /// </remarks>
    public interface IStorageTokenCredentialProvider
    {
        /// <summary>Gets the Entra credential used by the provider, or null for non-Entra authentication.</summary>
        TokenCredential? TokenCredential { get; }
    }
}
