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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventHubs
{
    internal class WorkQueue<T>
    {
        private readonly CancellationToken cancellationToken;
        private readonly object thisLock = new object();

        private readonly Queue<T> work = new Queue<T>();
        private readonly Queue<CancellableCompletionSource<T>> waiters = new Queue<CancellableCompletionSource<T>>();

        private readonly BatchTimer<CancellableCompletionSource<T>> expirationTimer;

        public WorkQueue(CancellationToken token, Action<List<CancellableCompletionSource<T>>> onExpiration)
        {
            this.cancellationToken = token;
            this.expirationTimer = new BatchTimer<CancellableCompletionSource<T>>(token, onExpiration);
        }

        public void Add(T element)
        {
            lock (thisLock)
            {
                // if there are waiters, hand it to the oldest one in line
                while (waiters.Count > 0)
                {
                    var next = waiters.Dequeue();
                    if (next.TrySetResult(element))
                    {
                        return;
                    }
                }
                    
                work.Enqueue(element);
            }
        }

        public Task<T> GetNext(TimeSpan timeout, CancellationToken cancellationToken)
        {
            lock (thisLock)
            {
                while (work.Count > 0)
                {
                    var next = work.Dequeue();
                    return Task.FromResult(next);
                }

                var tcs = new CancellableCompletionSource<T>(cancellationToken);

                this.expirationTimer.Schedule(DateTime.UtcNow + timeout, tcs);

                this.waiters.Enqueue(tcs);

                return tcs.Task;
            }
        }
    }
}
