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

namespace DurableTask.Test
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    internal class FakeOrchestrationClock
    {
        readonly object timersLock = new object();
        List<dynamic> timers = new List<dynamic>();

        public FakeOrchestrationClock()
        {
            CurrentUtcDateTime = new DateTime(0);
        }

        public DateTime CurrentUtcDateTime { get; internal set; }

        public bool HasPendingTimers
        {
            get { return timers.Count > 0; }
        }

        public async Task<T> CreateTimer<T>(DateTime fireAt, T state, CancellationToken cancelToken)
        {
            if (fireAt <= CurrentUtcDateTime)
            {
                return state;
            }

            var tcs = new TaskCompletionSource<object>();
            dynamic timerInfo = new {FireAt = fireAt, TaskCompletionSource = tcs};
            lock (timersLock)
            {
                timers.Add(timerInfo);
                timers = timers.OrderBy(t => t.FireAt).ToList();
            }

            if (cancelToken != CancellationToken.None)
            {
                cancelToken.Register(s =>
                {
                    if (tcs.TrySetCanceled())
                    {
                        lock (timersLock)
                        {
                            timers.Remove(timerInfo);
                        }
                    }
                }, tcs);
            }

            await tcs.Task;

            return state;
        }

        public TimeSpan FirePendingTimers()
        {
            while (true)
            {
                dynamic timerInfo = null;
                lock (timersLock)
                {
                    timerInfo = timers.FirstOrDefault();
                }

                if (timerInfo == null)
                {
                    return TimeSpan.Zero;
                }

                DateTime fireAt = timerInfo.FireAt;
                if (fireAt > CurrentUtcDateTime)
                {
                    return fireAt - CurrentUtcDateTime;
                }

                lock (timersLock)
                {
                    timers.Remove(timerInfo);
                }

                timerInfo.TaskCompletionSource.SetResult(null);
            }
        }

        public void MoveClockForward(TimeSpan span)
        {
            CurrentUtcDateTime = CurrentUtcDateTime.AddTicks(span.Ticks);
            while (true)
            {
                dynamic timerInfo = null;
                lock (timersLock)
                {
                    timerInfo = timers.FirstOrDefault();
                }

                if (timerInfo == null)
                {
                    break;
                }

                if (timerInfo.FireAt > CurrentUtcDateTime)
                {
                    break;
                }

                lock (timersLock)
                {
                    timers.Remove(timerInfo);
                }

                timerInfo.TaskCompletionSource.SetResult(null);
            }
        }
    }
}