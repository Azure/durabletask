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
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventHubs
{
    internal class SendWorker : Backend.ISender
    {
        private readonly object thisLock = new object();
        private readonly CancellationToken cancellationToken;
        private Func<List<Event>, Task> handler;
        private List<Event> work;
        private List<Backend.ISendConfirmationListener> listeners;

        public SendWorker(CancellationToken token, Func<List<Event>, Task> handler = null)
        {
            this.cancellationToken = token;
            this.handler = handler;
            this.work = new List<Event>();
            this.listeners = new List<Backend.ISendConfirmationListener>();
            this.thisLock = new object();

            if (handler != null)
            {
                this.SetHandler(handler);
            }

            cancellationToken.Register(Release);
        }

        private void Release()
        {
            lock(this.thisLock)
            {
                Monitor.Pulse(this.thisLock);
            }
        }

        public void SetHandler(Func<List<Event>, Task> handler)
        {
            if (this.handler != null)
            {
                throw new InvalidOperationException();
            }
            this.handler = handler ?? throw new ArgumentNullException(nameof(handler));
            new Thread(WorkLoop).Start();
        }

        void Backend.ISender.Submit(Event element, Backend.ISendConfirmationListener confirmationListener)
        {
            lock (this.thisLock)
            {
                if (this.work.Count == 0)
                {
                    Monitor.Pulse(this.thisLock);
                }

                this.work.Add(element);
                this.listeners.Add(confirmationListener);
            }
        }

        private void WorkLoop()
        {
            List<Event> batch = new List<Event>();
            List<Backend.ISendConfirmationListener> listeners = new List<Backend.ISendConfirmationListener>();

            while (!cancellationToken.IsCancellationRequested)
            {
                lock (this.thisLock)
                {
                    while (this.work.Count == 0 && !cancellationToken.IsCancellationRequested)
                    {
                        Monitor.Wait(this.thisLock);
                    }

                    var temp = this.work;
                    this.work = batch;
                    batch = temp;

                    var temp2 = this.listeners;
                    this.listeners = listeners;
                    listeners = temp2;
                }

                if (batch.Count > 0)
                {
                    try
                    {
                        handler(batch).Wait();
                    }
                    catch (Exception e)
                    {
                        System.Diagnostics.Trace.TraceError($"exception in send worker: {e}", e);
                    }

                    for (int i = 0; i < batch.Count; i++)
                    {
                        listeners[i]?.ConfirmDurablySent(batch[i]);
                    }

                    batch.Clear();
                    listeners.Clear();
                }
            }
        }
    }
}
