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

namespace DurableTask.Core.Common
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Simple MRU cache implementation. TODO, cgillum: Implement locking for thread safety!
    /// </summary>
    class MruCache<TKey, TValue> where TValue : class
    {
        readonly LinkedList<TKey> mruList;
        readonly Dictionary<TKey, CacheEntry> items;

        CacheEntry mruEntry;

        public MruCache(int watermark)
            : this(watermark * 4 / 5, watermark)
        {
        }

        // The cache will grow until the high watermark. At which point, the least recently used items
        // will be purge until the cache's size is reduced to low watermark
        public MruCache(int lowWatermark, int highWatermark)
            : this(lowWatermark, highWatermark, null)
        {
        }

        public MruCache(int lowWatermark, int highWatermark, IEqualityComparer<TKey> comparer)
        {
            System.Diagnostics.Debug.Assert(lowWatermark < highWatermark, "lowWatermark must be less than highWatermark");
            System.Diagnostics.Debug.Assert(lowWatermark >= 0, "lowWatermark must be greater than or equal to zero");

            this.LowWatermark = lowWatermark;
            this.HighWatermark = highWatermark;
            this.mruList = new LinkedList<TKey>();
            if (comparer == null)
            {
                this.items = new Dictionary<TKey, CacheEntry>();
            }
            else
            {
                this.items = new Dictionary<TKey, CacheEntry>(comparer);
            }
        }

        public int LowWatermark { get; }

        public int HighWatermark { get; }

        public void Add(TKey key, TValue value)
        {
            this.Add(key, value, DateTime.MaxValue);
        }

        public void Add(TKey key, TValue value, DateTime expiresAfter)
        {
            this.Add(key, value, expiresAfter, TimeSpan.Zero);
        }

        public void Add(TKey key, TValue value, TimeSpan timeToLive)
        {
            this.Add(key, value, DateTime.UtcNow.Add(timeToLive), timeToLive);
        }

        public void Add(TKey key, TValue value, DateTime expiresAfter, TimeSpan timeToLive)
        {
            // if anything goes wrong (duplicate entry, etc) we should 
            // clear our caches so that we don't get out of sync
            bool success = false;
            try
            {
                if (this.items.Count >= this.HighWatermark)
                {
                    // If the cache is full, purge enough LRU items to shrink the 
                    // cache down to the low watermark
                    int countToPurge = this.HighWatermark - this.LowWatermark;

                    var last = this.mruList.Last;
                    while (last != null && countToPurge > 0)
                    {
                        TKey keyToRemove = last.Value;
                        CacheEntry entryToRemove = this.items[keyToRemove];
                        if (entryToRemove.TimeToLiveTicks > 0 && entryToRemove.ExpiresAfterUtcTicks > DateTime.UtcNow.Ticks)
                        {
                            // Skip this item since it's in use.
                            last = last.Previous;
                            continue;
                        }

                        this.mruList.Remove(last);
                        TValue item = entryToRemove.Value;
                        this.items.Remove(keyToRemove);
                        this.OnSingleItemRemoved(item);

                        countToPurge--;
                        last = last.Previous;
                    }
                }

                // Add  the new entry to the cache and make it the MRU element
                CacheEntry entry;
                entry.Node = this.mruList.AddFirst(key);
                entry.Value = value;
                entry.ExpiresAfterUtcTicks = expiresAfter.ToUniversalTime().Ticks;
                entry.TimeToLiveTicks = timeToLive.Ticks;

                this.items.Add(key, entry);
                this.mruEntry = entry;
                success = true;
            }
            finally
            {
                if (!success)
                {
                    this.Clear();
                }
            }
        }

        public void Clear()
        {
            this.mruList.Clear();
            this.items.Clear();
            this.mruEntry.Value = null;
            this.mruEntry.Node = null;
        }

        public bool Remove(TKey key)
        {
            CacheEntry entry;
            if (this.items.TryGetValue(key, out entry))
            {
                this.items.Remove(key);
                this.OnSingleItemRemoved(entry.Value);
                this.mruList.Remove(entry.Node);
                if (object.ReferenceEquals(this.mruEntry.Node, entry.Node))
                {
                    this.mruEntry.Value = null;
                    this.mruEntry.Node = null;
                }

                return true;
            }

            return false;
        }

        // If found, make the entry most recently used
        public bool TryGetValue(TKey key, out TValue value)
        {
            // remove the MRU item if it is invalid
            if (this.mruEntry.Node != null && !IsValid(this.mruEntry))
            {
                this.Remove(this.mruEntry.Node.Value);
            }

            // first check our MRU item
            if (this.mruEntry.Node != null && key != null && key.Equals(this.mruEntry.Node.Value))
            {
                value = this.mruEntry.Value;
                return true;
            }

            CacheEntry entry;

            bool found = this.items.TryGetValue(key, out entry);
            if (found)
            {
                if (entry.TimeToLiveTicks > 0)
                {
                    entry.ExpiresAfterUtcTicks = DateTime.UtcNow.AddTicks(entry.TimeToLiveTicks).Ticks;
                }
                else if (!IsValid(entry))
                {
                    this.Remove(key);
                    found = false;
                }
            }

            value = entry.Value;

            // Move the node to the head of the MRU list if it's not already there
            if (found && this.mruList.Count > 1
                && !object.ReferenceEquals(this.mruList.First, entry.Node))
            {
                this.mruList.Remove(entry.Node);
                this.mruList.AddFirst(entry.Node);
                this.mruEntry = entry;
            }

            return found;
        }

        protected virtual void OnSingleItemRemoved(TValue item)
        {
            IDisposable disposable = item as IDisposable;
            if (disposable != null)
            {
                disposable.Dispose();
            }
        }

        static bool IsValid(CacheEntry item)
        {
            return DateTime.UtcNow.Ticks <= item.ExpiresAfterUtcTicks;
        }

        struct CacheEntry
        {
            internal TValue Value;
            internal LinkedListNode<TKey> Node;
            internal long ExpiresAfterUtcTicks;
            internal long TimeToLiveTicks;
        }
    }
}
