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
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;

    sealed class DispatchQueue : IDisposable
    {
        readonly SemaphoreSlim messagesSemaphore = new SemaphoreSlim(0);
        readonly ConcurrentQueue<Func<Task>> tasks = new ConcurrentQueue<Func<Task>>();
        readonly int dispatcherCount;
        int initialized;

        public DispatchQueue(int dispatcherCount)
        {
            this.dispatcherCount = dispatcherCount;
        }

        public void EnqueueAndDispatch(Func<Task> task)
        {
            this.tasks.Enqueue(task);
            this.messagesSemaphore.Release();

            if (Interlocked.CompareExchange(ref this.initialized, 1, 0) == 0)
            {
                for (int i = 0; i < this.dispatcherCount; i++)
                {
                    Task.Run(this.DispatchLoop);
                }
            }
        }

        async Task DispatchLoop()
        {
            while (true)
            {
                await this.messagesSemaphore.WaitAsync();

                if (this.tasks.TryDequeue(out Func<Task> task))
                {
                    await task();
                }
            }
        }

        public void Dispose()
        {
            this.messagesSemaphore.Dispose();
        }
    }
}
