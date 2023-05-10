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
    using System.Collections.Generic;

    internal class LRUCache<TKey, TValue>
    {
        readonly int capacity;
        readonly Dictionary<TKey, LinkedListNode<(TKey key, TValue value)>> cache;
        readonly LinkedList<(TKey key, TValue value)> list;
        readonly object syncLock = new object();

        public LRUCache(int capacity)
        {
            this.capacity = capacity;
            this.cache = new Dictionary<TKey, LinkedListNode<(TKey, TValue)>>();
            this.list = new LinkedList<(TKey, TValue)>();
        }

        public void Add(TKey key, TValue value)
        {
            lock (this.syncLock)
            {
                if (this.cache.TryGetValue(key, out var node))
                {
                    this.list.Remove(node);
                    this.list.AddFirst(node);
                    node.Value = (key, value);
                }
                else
                {
                    if (this.cache.Count == capacity)
                    {
                        var last = this.list.Last;
                        this.list.RemoveLast();
                        this.cache.Remove(last.Value.key);
                    }

                    var newNode = new LinkedListNode<(TKey, TValue)>((key, value));
                    this.list.AddFirst(newNode);
                    this.cache[key] = newNode;
                }
            }
        }

        public bool TryGet(TKey key, out TValue value)
        {
            lock (this.syncLock)
            {
                if (this.cache.TryGetValue(key, out var node))
                {
                    this.list.Remove(node);
                    this.list.AddFirst(node);
                    value = node.Value.value;
                    return true;
                }
                else
                {
                    value = default(TValue);
                    return false;
                }
            }
        }

        public bool TryRemove(TKey key)
        {
            lock (this.syncLock)
            {
                if (this.cache.TryGetValue(key, out var node))
                {
                    this.cache.Remove(key);
                    this.list.Remove(node);
                    return true;
                }

                return false;
            }
        }
    }
}
