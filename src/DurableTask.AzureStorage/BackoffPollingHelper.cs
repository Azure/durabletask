//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace DurableTask.AzureStorage
{
    using System;
    using System.Diagnostics;

    /// <summary>
    /// Utility class for implementing semi-intelligent backoff polling for Azure queues.
    /// </summary>
    class BackoffPollingHelper
    {
        readonly long maxDelayMs;
        readonly long minDelayThreasholdMs;
        readonly double sleepToWaitRatio;
        readonly Stopwatch stopwatch;

        public BackoffPollingHelper(TimeSpan maxDelay, TimeSpan minDelayThreshold, double sleepToWaitRatio = 0.1)
        {
            Debug.Assert(maxDelay >= TimeSpan.Zero);
            Debug.Assert(sleepToWaitRatio > 0 && sleepToWaitRatio <= 1);

            this.maxDelayMs = (long)maxDelay.TotalMilliseconds;
            this.minDelayThreasholdMs = (long)minDelayThreshold.TotalMilliseconds;
            this.sleepToWaitRatio = sleepToWaitRatio;
            this.stopwatch = new Stopwatch();
        }

        public void Reset()
        {
            this.stopwatch.Reset();
        }

        public TimeSpan GetNextDelay()
        {
            if (!this.stopwatch.IsRunning)
            {
                this.stopwatch.Start();
            }

            long elapsedMs = this.stopwatch.ElapsedMilliseconds;
            double sleepDelayMs = elapsedMs * this.sleepToWaitRatio;
            if (sleepDelayMs < minDelayThreasholdMs)
            {
                return TimeSpan.Zero;
            }

            if (sleepDelayMs > this.maxDelayMs)
            {
                sleepDelayMs = this.maxDelayMs;
            }

            return TimeSpan.FromMilliseconds(sleepDelayMs);
        }
    }
}
