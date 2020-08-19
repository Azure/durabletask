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
    using System.Threading.Tasks;

    sealed class LeaseObserver<T> where T : Lease
    {
        private readonly Func<T, Task> leaseAquiredDelegate;
        private readonly Func<T, CloseReason, Task> leaseReleasedDelegate;

        public LeaseObserver(
            Func<T, Task> leaseAquiredDelegate,
            Func<T, CloseReason, Task> leaseReleasedDelegate)
        {
            this.leaseAquiredDelegate = leaseAquiredDelegate;
            this.leaseReleasedDelegate = leaseReleasedDelegate;
        }

        public Task OnLeaseAquiredAsync(T lease)
        {
            return leaseAquiredDelegate.Invoke(lease);
        }

        public Task OnLeaseReleasedAsync(T lease, CloseReason reason)
        {
            return leaseReleasedDelegate.Invoke(lease, reason);
        }
    }
}