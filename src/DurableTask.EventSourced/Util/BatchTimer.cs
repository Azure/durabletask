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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventSourced
{
    internal class BatchTimer<T> : IComparer<DateTime>
    {
        private readonly object thisLock = new object();
        private readonly CancellationToken cancellationToken;
        private readonly Action<List<T>> handler;
        private readonly SortedList<DateTime, T> schedule;
        private readonly SemaphoreSlim notify;

        //TODO implement this using linked list so we can collect garbage

        public BatchTimer(CancellationToken token, Action<List<T>> handler)
        {
            this.cancellationToken = token;
            this.handler = handler;
            this.schedule = new SortedList<DateTime, T>(this);
            this.notify = new SemaphoreSlim(0, int.MaxValue);

            new Thread(ExpirationCheckLoop).Start();

            token.Register(() => this.notify.Release());
        }

        public int Compare(DateTime x, DateTime y)
        {
            int result = x.CompareTo(y);

            if (result == 0)
                return 1;   // Handle equality as being greater, so that the list can contain duplicate keys
            else
                return result;
        }

        public void Schedule(DateTime when, T what)
        {
            lock (this.schedule)
            {
                this.schedule.Add(when, what);

                // notify the expiration check loop
                if (when == this.schedule.First().Key)
                {
                    this.notify.Release();
                }
            }
        }

        private void ExpirationCheckLoop()
        {
            List<T> batch = new List<T>();

            while (!cancellationToken.IsCancellationRequested)
            {
                // wait for the next expiration time or cleanup, but cut the wait short if notified
                if (this.RequiresDelay(out var delay))
                {
                    this.notify.Wait(delay); // blocks thread until delay is over, or until notified
                }

                lock (this.schedule)
                {
                    var next = this.schedule.FirstOrDefault();

                    while (this.schedule.Count > 0
                        && next.Key <= DateTime.UtcNow
                        && !this.cancellationToken.IsCancellationRequested)
                    {
                        this.schedule.RemoveAt(0);
                        batch.Add(next.Value);

                        next = this.schedule.FirstOrDefault();
                    }
                }

                if (batch.Count > 0)
                {
                    try
                    {
                        handler(batch);
                    }
                    catch
                    {
                        //TODO
                    }

                    batch.Clear();
                }
            }
        }

        private bool RequiresDelay(out TimeSpan delay)
        {
            lock (this.schedule)
            {
                if (this.schedule.Count == 0)
                {
                    delay = TimeSpan.FromMilliseconds(-1); // represents infinite delay
                    return true;
                }

                var next = this.schedule.First();
                var now = DateTime.UtcNow;

                if (next.Key > now)
                {
                    delay = next.Key - now;
                    return true;
                }
                else
                {
                    delay = default;
                    return false;
                }
            }
        }
    }
}
