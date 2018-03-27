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

namespace DurableTask.Emulator
{
    using DurableTask.Core;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    internal class PeeklockQueue
    {
        List<TaskMessage> messages;
        HashSet<TaskMessage> lockTable;

        readonly object thisLock = new object();


        public PeeklockQueue()
        {
            this.messages = new List<TaskMessage>();
            this.lockTable = new HashSet<TaskMessage>();
        }

        public async Task<TaskMessage> ReceiveMessageAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            Stopwatch timer = Stopwatch.StartNew();
            while (timer.Elapsed < receiveTimeout && !cancellationToken.IsCancellationRequested)
            {
                lock(this.thisLock)
                {
                    foreach (TaskMessage tm in this.messages)
                    {
                        if (!this.lockTable.Contains(tm))
                        {
                            this.lockTable.Add(tm);
                            return tm;
                        }
                    }
                }

                await Task.Delay(TimeSpan.FromMilliseconds(500));
            }

            if (cancellationToken.IsCancellationRequested)
            {
                throw new TaskCanceledException();
            }

            return null;
        }

        public void SendMessageAsync(TaskMessage message)
        {
            lock(this.thisLock)
            {
                this.messages.Add(message);
            }
        }

        public void CompleteMessageAsync(TaskMessage message)
        {
            lock(this.thisLock)
            {
                if(!this.lockTable.Contains(message))
                {
                    throw new InvalidOperationException("Message Lock Lost");
                }

                this.lockTable.Remove(message);
                this.messages.Remove(message);
            }
        }

        public void AbandonMessageAsync(TaskMessage message)
        {
            lock (this.thisLock)
            {
                if (!this.lockTable.Contains(message))
                {
                    return;
                }

                this.lockTable.Remove(message);
            }
        }
    }
}