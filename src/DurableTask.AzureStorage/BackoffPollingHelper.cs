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
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Utility for implementing semi-intelligent backoff polling for Azure queues.
    /// </summary>
    class BackoffPollingHelper
    {
        readonly long maxDelayMs;
        readonly long minDelayThreasholdMs;
        readonly double sleepToWaitRatio;
        readonly Stopwatch stopwatch;

        TimeSpan delayTimeout;
        AsyncAutoResetEvent resetEvent;

        public BackoffPollingHelper(TimeSpan maxDelay, TimeSpan minDelayThreshold)
            : this(maxDelay, minDelayThreshold, sleepToWaitRatio: 0.1)
        {
        }

        public BackoffPollingHelper(TimeSpan maxDelay, TimeSpan minDelayThreshold, double sleepToWaitRatio)
        {
            Debug.Assert(maxDelay >= TimeSpan.Zero);
            Debug.Assert(sleepToWaitRatio > 0 && sleepToWaitRatio <= 1);

            this.maxDelayMs = (long)maxDelay.TotalMilliseconds;
            this.minDelayThreasholdMs = (long)minDelayThreshold.TotalMilliseconds;
            this.sleepToWaitRatio = sleepToWaitRatio;
            this.stopwatch = new Stopwatch();
            this.resetEvent = new AsyncAutoResetEvent(signaled: false);
            this.delayTimeout = TimeSpan.Zero;
        }

        public void Reset()
        {
            this.stopwatch.Reset();
            this.resetEvent.Set();
        }

        public async Task<bool> WaitAsync(CancellationToken hostCancellationToken)
        {
            bool signaled = await this.resetEvent.WaitAsync(this.delayTimeout, hostCancellationToken);
            if (!signaled)
            {
                this.IncreaseDelay();
            }

            return signaled;
        }

        private void IncreaseDelay()
        {
            if (!this.stopwatch.IsRunning)
            {
                this.stopwatch.Start();
            }

            long elapsedMs = this.stopwatch.ElapsedMilliseconds;
            double sleepDelayMs = elapsedMs * this.sleepToWaitRatio;

            TimeSpan nextDelay;
            if (sleepDelayMs < minDelayThreasholdMs)
            {
                nextDelay = TimeSpan.Zero;
            }
            else
            {
                if (sleepDelayMs > this.maxDelayMs)
                {
                    sleepDelayMs = this.maxDelayMs;
                }

                nextDelay = TimeSpan.FromMilliseconds(sleepDelayMs);
            }

            this.delayTimeout = nextDelay;
        }
    }
}
