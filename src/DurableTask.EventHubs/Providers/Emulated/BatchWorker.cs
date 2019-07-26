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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DurableTask.EventHubs
{
    internal class BatchWorker<T> : Backend.ISender<T>
    {
        private readonly object thisLock = new object();
        private readonly CancellationToken cancellationToken;
        private Func<List<T>, Task> handler;
        private List<T> work;

        public Action<T> SubmitTracer { get; set; }

        public BatchWorker(CancellationToken token, Func<List<T>, Task> handler = null)
        {
            this.cancellationToken = token;
            this.handler = handler;
            this.work = new List<T>();
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

        public void SetHandler(Func<List<T>, Task> handler)
        {
            if (this.handler != null)
            {
                throw new InvalidOperationException();
            }
            this.handler = handler ?? throw new ArgumentNullException(nameof(handler));
            new Thread(WorkLoop).Start();
        }

        public void Submit(T what)
        {
            lock (this.thisLock)
            {
                if (this.work.Count == 0)
                {
                    Monitor.Pulse(this.thisLock);
                }

                this.work.Add(what);

                this.SubmitTracer?.Invoke(what);
            }
        }

        public void Submit(IEnumerable<T> what)
        {
            lock (this.thisLock)
            {
                if (this.work.Count == 0)
                {
                    Monitor.Pulse(this.thisLock);
                }

                foreach (var element in what)
                {
                    this.work.Add(element);
                    this.SubmitTracer?.Invoke(element);
                }
            }
        }

        private void WorkLoop()
        {
            List<T> batch = new List<T>();

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

                    batch.Clear();
                }
            }
        }
    }
}
