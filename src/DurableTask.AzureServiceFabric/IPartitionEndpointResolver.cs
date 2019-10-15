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

namespace DurableTask.AzureServiceFabric
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Provides partition related information.
    /// </summary>
    public interface IPartitionEndpointResolver
    {
        /// <summary>
        /// Gets end points for all the partitions.
        /// </summary>
        /// <param name="cancellationToken">Token to inform when a task is cancelled.</param>
        /// <returns> All the end points. </returns>
        Task<IEnumerable<string>> GetPartitionEndpointsAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Gets partition end point for given instanceId.
        /// </summary>
        /// <param name="instanceId">InstanceId of orchestration</param>
        /// <param name="cancellationToken">Token to inform when a task is cancelled.</param>
        /// <returns> Partition end point </returns>
        Task<string> GetPartitionEndPointAsync(string instanceId, CancellationToken cancellationToken);
    }
}
