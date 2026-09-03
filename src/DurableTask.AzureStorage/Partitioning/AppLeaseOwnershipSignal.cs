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

namespace DurableTask.AzureStorage.Partitioning
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    sealed class AppLeaseOwnershipSignal
    {
        readonly object syncLock = new object();

        TaskCompletionSource<object> ownershipAvailable = CreateCompletionSource();
        OwnershipEpoch currentEpoch;

        public void Set()
        {
            TaskCompletionSource<object> available;
            lock (this.syncLock)
            {
                if (this.currentEpoch != null)
                {
                    return;
                }

                this.currentEpoch = new OwnershipEpoch();
                available = this.ownershipAvailable;
            }

            available.TrySetResult(null);
        }

        public void Reset()
        {
            OwnershipEpoch epoch;
            lock (this.syncLock)
            {
                epoch = this.currentEpoch;
                if (epoch == null)
                {
                    return;
                }

                this.currentEpoch = null;
                this.ownershipAvailable = CreateCompletionSource();
            }

            epoch.Deactivate();
        }

        public async Task<AppLeaseOwnership> WaitAsync(CancellationToken cancellationToken)
        {
            while (true)
            {
                Task availableTask;
                lock (this.syncLock)
                {
                    AppLeaseOwnership ownership = this.currentEpoch?.TryAcquire();
                    if (ownership != null)
                    {
                        return ownership;
                    }

                    availableTask = this.ownershipAvailable.Task;
                }

                var canceled = new TaskCompletionSource<object>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                using (cancellationToken.Register(() => canceled.TrySetCanceled()))
                {
                    await Task.WhenAny(availableTask, canceled.Task);
                }

                cancellationToken.ThrowIfCancellationRequested();
            }
        }

        static TaskCompletionSource<object> CreateCompletionSource()
        {
            return new TaskCompletionSource<object>(
                TaskCreationOptions.RunContinuationsAsynchronously);
        }

        internal sealed class OwnershipEpoch
        {
            readonly object syncLock = new object();
            readonly CancellationTokenSource lostSource = new CancellationTokenSource();

            bool active = true;
            int referenceCount = 1;

            public AppLeaseOwnership TryAcquire()
            {
                lock (this.syncLock)
                {
                    if (!this.active)
                    {
                        return null;
                    }

                    this.referenceCount++;
                    return new AppLeaseOwnership(this);
                }
            }

            public bool TryBeginDispatch()
            {
                lock (this.syncLock)
                {
                    return this.active;
                }
            }

            public void Deactivate()
            {
                lock (this.syncLock)
                {
                    if (!this.active)
                    {
                        return;
                    }

                    this.active = false;
                }

                this.lostSource.Cancel();
                this.Release();
            }

            public void Release()
            {
                bool dispose;
                lock (this.syncLock)
                {
                    this.referenceCount--;
                    dispose = this.referenceCount == 0;
                }

                if (dispose)
                {
                    this.lostSource.Dispose();
                }
            }

            public CancellationToken LostToken => this.lostSource.Token;
        }

        public sealed class AppLeaseOwnership : IDisposable
        {
            OwnershipEpoch epoch;

            internal AppLeaseOwnership(OwnershipEpoch epoch)
            {
                this.epoch = epoch;
            }

            public CancellationToken LostToken => this.epoch.LostToken;

            public bool TryBeginDispatch()
            {
                return this.epoch.TryBeginDispatch();
            }

            public void Dispose()
            {
                OwnershipEpoch epoch = Interlocked.Exchange(ref this.epoch, null);
                epoch?.Release();
            }
        }
    }
}
