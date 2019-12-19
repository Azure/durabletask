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

namespace DurableTask.AzureServiceFabric.Stores
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Data;

    class SessionMessageProvider : MessageProviderBase<Guid, TaskMessageItem>
    {
        public SessionMessageProvider(IReliableStateManager stateManager, string storeName, CancellationToken token)
            : base(stateManager, storeName, token)
        {
        }

        protected override void AddItemInMemory(Guid key, TaskMessageItem value)
        {
            throw new NotSupportedException();
        }

        public override Task StartAsync()
        {
            return InitializeStore();
        }

        public async Task<List<Message<Guid, TaskMessageItem>>> ReceiveBatchAsync()
        {
            List<Message<Guid, TaskMessageItem>> result = new List<Message<Guid, TaskMessageItem>>();
            if (!IsStopped())
            {
                await this.EnumerateItems(kvp => result.Add(new Message<Guid, TaskMessageItem>(kvp.Key, kvp.Value)));
            }
            return result;
        }
    }
}
