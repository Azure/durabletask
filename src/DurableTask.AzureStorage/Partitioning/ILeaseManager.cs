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
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    interface ILeaseManager<T> where T : Lease
    {
        Task<bool> LeaseStoreExistsAsync(CancellationToken cancellationToken = default);

        Task<bool> CreateLeaseStoreIfNotExistsAsync(TaskHubInfo eventHubInfo, bool checkIfStale = true, CancellationToken cancellationToken = default);

        IAsyncEnumerable<T> ListLeasesAsync(CancellationToken cancellationToken = default);

        Task CreateLeaseIfNotExistAsync(string partitionId, CancellationToken cancellationToken = default);

        Task<T> GetLeaseAsync(string partitionId, CancellationToken cancellationToken = default);

        Task<bool> RenewAsync(T lease, CancellationToken cancellationToken = default);

        Task<bool> AcquireAsync(T lease, string owner, CancellationToken cancellationToken = default);

        Task<bool> ReleaseAsync(T lease, CancellationToken cancellationToken = default);

        Task DeleteAsync(T lease, CancellationToken cancellationToken = default);

        Task DeleteAllAsync(CancellationToken cancellationToken = default);

        Task<bool> UpdateAsync(T lease, CancellationToken cancellationToken = default);
    }
}
