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

namespace DurableTask.ServiceFabric
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ServiceFabric.Data;
    using Microsoft.ServiceFabric.Data.Collections;

    class SessionMessagesProvider<TKey, TValue> : MessageProviderBase<TKey, TValue> where TKey : IComparable<TKey>, IEquatable<TKey>
    {
        public SessionMessagesProvider(IReliableStateManager stateManager, string storeName, CancellationToken token)
            : base(stateManager, storeName, token)
        {
        }

        protected override void AddItemInMemory(TKey key, TValue value)
        {
            throw new NotSupportedException();
        }

        public override Task StartAsync()
        {
            return InitializeStore();
        }

        public async Task<List<Message<TKey, TValue>>> ReceiveBatchAsync()
        {
            List<Message<TKey, TValue>> result = new List<Message<TKey, TValue>>();
            if (!IsStopped())
            {
                using (var tx = this.StateManager.CreateTransaction())
                {
                    var count = await this.Store.GetCountAsync(tx);

                    if (count > 0)
                    {
                        var enumerable = await this.Store.CreateEnumerableAsync(tx, EnumerationMode.Unordered);
                        using (var enumerator = enumerable.GetAsyncEnumerator())
                        {
                            while (await enumerator.MoveNextAsync(this.CancellationToken))
                            {
                                result.Add(new Message<TKey, TValue>(enumerator.Current.Key, enumerator.Current.Value));
                            }
                        }
                    }
                }
            }
            return result;
        }
    }
}
