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

    /// <summary>
    /// Options to control various aspects of partition distribution happening within the host instance.
    /// </summary> 
    class LeaseCollectionBalancerOptions
    {
        /// <summary>
        /// Renew interval for all leases for partitions currently held by this instance.
        /// </summary>
        /// <remarks>
        /// The table partition manager does not use this setting. It instead relies on <see cref="AcquireInterval"/>
        /// to determine the interval for renewing partition leases.
        /// </remarks>
        public TimeSpan RenewInterval { get; set; } = TimeSpan.FromSeconds(10);

        /// <summary>
        /// Interval when this instance kicks off a task to compute if partitions are distributed evenly
        /// among known host instances. 
        /// </summary>
        /// <remarks>
        /// When using the table partition manager, this property sets the frequency at which the
        /// worker reads and updates the partition table except in the following two scenarios:
        /// (1) If the worker fails to update the partition table, then the partitions table is read immediately.
        /// (2) If the worker is waiting for a partition to be released or is working on releasing a partition, then the interval becomes 1 second.
        /// </remarks>
        public TimeSpan AcquireInterval { get; set; } = TimeSpan.FromSeconds(10);

        /// <summary>
        /// Interval for which the lease is taken.  If the lease is not renewed within this 
        /// interval, the lease will expire and ownership of the partition will move to another instance.
        /// </summary>
        public TimeSpan LeaseInterval { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Determines whether or not this set of leases should utilize lease stealing logic for rebalancing
        /// during scale-out operations.
        /// </summary>
        public bool ShouldStealLeases { get; set;} = true;

        /// <summary>
        /// Creates an instance of <see cref="LeaseCollectionBalancerOptions"/> with following default values:
        ///     a) RenewInterval = 10 seconds
        ///     b) AcquireInterval = 10 seconds
        ///     c) DefaultLeaseInterval = 30 seconds
        ///     d) MaxReceiveClients = 16,
        /// </summary>
        public static LeaseCollectionBalancerOptions DefaultOptions
        {
            get
            {
                return new LeaseCollectionBalancerOptions();
            }
        }
    }
}
