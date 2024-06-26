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
    /// Represents a factory for a particular Azure Storage service client.
    /// </summary>
    /// <typeparam name="TClient">The type of the client.</typeparam>
    /// <typeparam name="TClientOptions">The type of the options used by the client.</typeparam>
    public interface IStorageServiceClientProvider<TClient, TClientOptions> where TClientOptions : ClientOptions
    {
        /// <summary>
        /// Creates the options for the client.
        /// </summary>
        /// <remarks>
        /// The result may be modified by callers before invoking <see cref="CreateClient(TClientOptions)"/>.
        /// </remarks>
        /// <returns>The corresponding client options.</returns>
        TClientOptions CreateOptions();

        /// <summary>
        /// Creates the client based on the given <paramref name="options"/>.
        /// </summary>
        /// <param name="options">Options for the client.</param>
        /// <returns>The corresponding client.</returns>
        TClient CreateClient(TClientOptions options);
    }
}
