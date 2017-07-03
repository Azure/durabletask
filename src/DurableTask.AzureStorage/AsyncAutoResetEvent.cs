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
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    // Inspired by https://blogs.msdn.microsoft.com/pfxteam/2012/02/11/building-async-coordination-primitives-part-2-asyncautoresetevent/
    class AsyncAutoResetEvent
    {
        readonly LinkedList<TaskCompletionSource<bool>> waiters = 
            new LinkedList<TaskCompletionSource<bool>>();

        bool isSignaled;

        public AsyncAutoResetEvent(bool signaled)
        {
            this.isSignaled = signaled;
        }

        public Task<bool> WaitAsync(TimeSpan timeout)
        {
            return this.WaitAsync(timeout, CancellationToken.None);
        }

        public async Task<bool> WaitAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            TaskCompletionSource<bool> tcs;

            lock (this.waiters)
            {
                if (this.isSignaled)
                {
                    this.isSignaled = false;
                    return true;
                }
                else if (timeout == TimeSpan.Zero)
                {
                    return this.isSignaled;
                }
                else
                {
                    tcs = new TaskCompletionSource<bool>();
                    this.waiters.AddLast(tcs);
                }
            }

            Task winner = await Task.WhenAny(tcs.Task, Task.Delay(timeout, cancellationToken));
            if (winner == tcs.Task)
            {
                // The task was signaled.
                return true;
            }
            else
            {
                // We timed-out; remove our reference to the task.
                // This is an O(n) operation since waiters is a LinkedList<T>.
                lock (this.waiters)
                {
                    bool removed = this.waiters.Remove(tcs);
                    Debug.Assert(removed);
                    return false;
                }
            }
        }

        public void Set()
        {
            TaskCompletionSource<bool> toRelease = null;

            lock (this.waiters)
            {
                if (this.waiters.Count > 0)
                {
                    // Signal the first task in the waiters list.
                    toRelease = this.waiters.First.Value;
                    this.waiters.RemoveFirst();
                }
                else if (!this.isSignaled)
                {
                    // No tasks are pending
                    this.isSignaled = true;
                }
            }

            if (toRelease != null)
            {
                toRelease.SetResult(true);
            }
        }
    }
}
