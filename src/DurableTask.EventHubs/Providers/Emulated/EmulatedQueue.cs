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
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventHubs
{
    /// <summary>
    /// Simulates a in-memory queue for delivering events. Used for local testing and debugging.
    /// </summary>
    internal class EmulatedQueue<T> where T : Event
    {
        private readonly TimeSpan pollInterval;
        private readonly List<T> messages = new List<T>();
        private readonly CancellationToken cancellationToken;

        public EmulatedQueue(TimeSpan pollInterval, CancellationToken cancellationToken)
        {
            this.pollInterval = pollInterval;
            this.cancellationToken = cancellationToken;
        }

        public async Task<List<T>> ReceiveBatchAsync(long fromPosition)
        {
            while (true)
            {
                await Task.Delay(pollInterval, this.cancellationToken);

                if (this.cancellationToken.IsCancellationRequested)
                {
                    return null;
                }

                lock (messages)
                {
                    if (this.messages.Count > fromPosition)
                    {
                        return this.messages.GetRange((int)fromPosition, (int)(this.messages.Count - fromPosition));
                    }
                }
            }
        }

        public async Task SendAsync(T @event)
        {
            await Task.Delay(pollInterval);

            lock (messages)
            {
                this.messages.Add(@event);
            }
        }
    }
}
