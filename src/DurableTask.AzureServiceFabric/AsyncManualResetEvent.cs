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

namespace DurableTask.AzureServiceFabric
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    sealed class AsyncManualResetEvent
    {
        TaskCompletionSource<bool> taskCompletionSource = new TaskCompletionSource<bool>();

        public async Task<bool> WaitAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            if (timeout < TimeSpan.Zero)
            {
                timeout = TimeSpan.Zero;
            }

            var delayTask = Task.Delay(timeout, cancellationToken);
            var resultTask = await Task.WhenAny(delayTask, this.taskCompletionSource.Task);
            return delayTask != resultTask;
        }

        public void Set()
        {
            this.taskCompletionSource.TrySetResult(true);
        }

        public void Reset()
        {
            while (true)
            {
                var thisTcs = this.taskCompletionSource;

                if (!thisTcs.Task.IsCompleted || Interlocked.CompareExchange(ref this.taskCompletionSource, new TaskCompletionSource<bool>(), thisTcs) == thisTcs)
                {
                    return;
                }
            }
        }
    }
}
