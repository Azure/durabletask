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
    using DurableTask.Core.History;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    internal class PeekLockSessionQueue
    {
        readonly List<TaskSession> sessionQueue;
        readonly List<TaskSession> lockedSessionQueue;

        readonly object thisLock = new object();

        public PeekLockSessionQueue()
        {
            this.sessionQueue = new List<TaskSession>();
            this.lockedSessionQueue = new List<TaskSession>();
        }

        public void DropSession(string id)
        {
            lock (this.thisLock)
            {
                TaskSession taskSession = this.lockedSessionQueue.Find((ts) => string.Equals(ts.Id, id, StringComparison.InvariantCultureIgnoreCase));

                if (taskSession == null)
                {
                    return;
                }

                if (this.sessionQueue.Contains(taskSession))
                {
                    this.sessionQueue.Remove(taskSession);
                }
                else if (this.lockedSessionQueue.Contains(taskSession))
                {
                    this.lockedSessionQueue.Remove(taskSession);
                }
            }
        }

        public void SendMessage(TaskMessage message)
        {
            lock (this.thisLock)
            {
                foreach (TaskSession ts in this.sessionQueue)
                {
                    if (ts.Id == message.OrchestrationInstance.InstanceId)
                    {
                        ts.Messages.Add(message);
                        return;
                    }
                }

                foreach (TaskSession ts in this.lockedSessionQueue)
                {
                    if (ts.Id == message.OrchestrationInstance.InstanceId)
                    {
                        ts.Messages.Add(message);
                        return;
                    }
                }

                // create a new session
                this.sessionQueue.Add(new TaskSession
                {
                    Id = message.OrchestrationInstance.InstanceId,
                    SessionState = null,
                    Messages = new List<TaskMessage> { message }
                });
            }
        }

        public void CompleteSession(
            string id,
            byte[] newState,
            IList<TaskMessage> newMessages,
            TaskMessage continuedAsNewMessage)
        {
            lock (this.thisLock)
            {
                TaskSession taskSession = this.lockedSessionQueue.Find((ts) => string.Equals(ts.Id, id, StringComparison.InvariantCultureIgnoreCase));

                if (taskSession == null)
                {
                    // TODO : throw proper lock lost exception (AFFANDAR)
                    throw new InvalidOperationException("Lock lost");
                }

                this.lockedSessionQueue.Remove(taskSession);

                // make the required updates to the session
                foreach (TaskMessage tm in taskSession.LockTable)
                {
                    taskSession.Messages.Remove(tm);
                }

                taskSession.LockTable.Clear();

                taskSession.SessionState = newState;

                if (newState != null)
                {
                    this.sessionQueue.Add(taskSession);
                }

                foreach (TaskMessage m in newMessages)
                {
                    SendMessage(m);
                }

                if (continuedAsNewMessage != null)
                {
                    SendMessage(continuedAsNewMessage);
                }
            }
        }

        public void AbandonSession(string id)
        {
            lock (this.thisLock)
            {
                TaskSession taskSession = this.lockedSessionQueue.Find((ts) => string.Equals(ts.Id, id, StringComparison.InvariantCultureIgnoreCase));

                if (taskSession == null)
                {
                    // TODO : throw proper lock lost exception (AFFANDAR)
                    throw new InvalidOperationException("Lock lost");
                }

                this.lockedSessionQueue.Remove(taskSession);

                // TODO : note that this we are adding to the tail of the queue rather than head, which is what sbus would actually do (AFFANDAR)
                //      doesn't really matter though in terms of semantics
                this.sessionQueue.Add(taskSession);

                // unlock all messages
                taskSession.LockTable.Clear();
            }
        }

        public async Task<TaskSession> AcceptSessionAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            Stopwatch timer = Stopwatch.StartNew();
            while (timer.Elapsed < receiveTimeout && !cancellationToken.IsCancellationRequested)
            {
                lock (this.thisLock)
                {
                    foreach (TaskSession ts in this.sessionQueue)
                    {
                        if (ts.Messages.Count > 0 && !ts.Messages.Any(m => m.Event is ExecutionStartedEvent ese && ese.ScheduledStartTime > DateTime.UtcNow))
                        {
                            this.lockedSessionQueue.Add(ts);
                            this.sessionQueue.Remove(ts);

                            // all messages are now locked
                            foreach (TaskMessage tm in ts.Messages)
                            {
                                ts.LockTable.Add(tm);
                            }

                            return ts;
                        }
                    }
                }

                await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            }

            if (cancellationToken.IsCancellationRequested)
            {
                throw new TaskCanceledException();
            }

            return null;
        }
    }
}
