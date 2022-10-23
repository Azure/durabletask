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
    using System;
    using Azure.Core;

    sealed class DefaultStorageServiceClientProvider<TClient, TClientOptions> : IStorageServiceClientProvider<TClient, TClientOptions> where TClientOptions : ClientOptions
    {
        readonly Func<TClientOptions, TClient> factory;
        readonly TClientOptions options;

        public DefaultStorageServiceClientProvider(Func<TClientOptions, TClient> factory, TClientOptions options)
        {
            this.factory = factory ?? throw new ArgumentNullException(nameof(factory));
            this.options = options ?? throw new ArgumentNullException(nameof(options));
        }

        public TClient CreateClient(TClientOptions options)
        {
            return this.factory(options);
        }

        public TClientOptions CreateOptions()
        {
            return this.options;
        }
    }
}
