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

namespace DurableTask.Common
{
    using System;
    using System.Collections;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    // TODO: Unit tests
    // REVIEW: Is this thread safe?
    class KeyedAsyncQueue<TKey, TValue> : IReadOnlyDictionary<TKey, TValue>, IEnumerable<TValue>
    {
        readonly Func<TValue, TKey> keySelector;
        readonly ConcurrentDictionary<TKey, TValue> innerDictionary;
        readonly AsyncQueue<TValue> innerQueue;

        public KeyedAsyncQueue(Func<TValue, TKey> keySelector)
            : this(keySelector, EqualityComparer<TKey>.Default)
        {
        }

        public KeyedAsyncQueue(Func<TValue, TKey> keySelector, IEqualityComparer<TKey> keyComparer)
        {
            this.keySelector = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
            this.innerQueue = new AsyncQueue<TValue>();
            this.innerDictionary = new ConcurrentDictionary<TKey, TValue>(keyComparer);
        }

        public TValue this[TKey key] => this.innerDictionary[key];

        public IEnumerable<TKey> Keys => this.innerDictionary.Keys;

        public IEnumerable<TValue> Values => this.innerQueue;

        public int Count => this.innerDictionary.Count;

        public bool TryAdd(TValue item)
        {
            TKey key = GetKeyOrThrow(item);
            if (this.innerDictionary.TryAdd(key, item))
            {
                this.innerQueue.Enqueue(item);
                return true;
            }

            return false;
        }

        public async Task<TValue> TakeAsync(CancellationToken cancellationToken)
        {
            TValue item = await this.innerQueue.DequeueAsync(cancellationToken);

            TKey key = GetKeyOrThrow(item);
            this.innerDictionary.TryRemove(GetKeyOrThrow(item), out TValue removed);

            System.Diagnostics.Debug.Assert(ReferenceEquals(item, removed));
            return item;
        }

        TKey GetKeyOrThrow(TValue item)
        {
            if (item == null)
            {
                throw new ArgumentNullException(nameof(item));
            }

            TKey key = this.keySelector(item);
            if (key == null)
            {
                throw new ArgumentException("The added item does not have a key.", nameof(item));
            }

            return key;
        }

        public bool ContainsKey(TKey key) => this.innerDictionary.ContainsKey(key);

        public bool TryGetValue(TKey key, out TValue value) => this.innerDictionary.TryGetValue(key, out value);

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() => this.innerDictionary.GetEnumerator();

        IEnumerator<TValue> IEnumerable<TValue>.GetEnumerator() => this.innerQueue.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => this.GetEnumerator();
    }
}
