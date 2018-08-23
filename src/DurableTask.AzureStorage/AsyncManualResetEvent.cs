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
    using System.Threading;
    using System.Threading.Tasks;

    class AsyncManualResetEvent
    {
        readonly object mutex = new object();
        TaskCompletionSource<object> tcs = new TaskCompletionSource<object>();

        public async Task<bool> WaitAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            Task delayTask = Task.Delay(timeout, cancellationToken);
            Task waitTask = this.tcs.Task;

            return await Task.WhenAny(waitTask, delayTask) == waitTask;
        }

        public void Set()
        {
            Task.Run(() =>
            {
                lock (this.mutex)
                {
                    this.tcs.TrySetResult(null);
                }
            });
        }

        public void Reset()
        {
            lock (this.mutex)
            {
                if (this.tcs.Task.IsCompleted)
                {
                    this.tcs = new TaskCompletionSource<object>();
                }
            }
        }
    }
}
