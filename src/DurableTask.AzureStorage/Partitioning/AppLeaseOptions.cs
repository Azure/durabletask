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
    /// Options to control timing intervals for the app lease.
    /// </summary> 
    public class AppLeaseOptions
    {
        /// <summary>
        /// Renew interval for the app lease held by this instance.
        /// </summary>
        public TimeSpan RenewInterval { get; set; } = TimeSpan.FromSeconds(25);

        /// <summary>
        /// Interval when this instance attempts to continuously acquire the app lease. When two applications are running, this setting
        /// will affect the secondary app attempting to take the app lease from the first app.
        /// </summary>
        public TimeSpan AcquireInterval { get; set; } = TimeSpan.FromSeconds(300);

        /// <summary>
        /// Interval for which the lease is taken on Azure Blob representing an EventHub partition.  If the lease is not renewed within this 
        /// interval, it will cause it to expire and ownership of the app lease will move to another instance.
        /// </summary>
        public TimeSpan LeaseInterval { get; set; } = TimeSpan.FromSeconds(60);

        /// <summary>
        /// Creates an instance of <see cref="AppLeaseOptions"/> with following default values:
        ///     a) RenewInterval = 25 seconds
        ///     b) AcquireInterval = 300 seconds, or 5 minutes
        ///     c) DefaultLeaseInterval = 60 seconds
        /// </summary>
        public static AppLeaseOptions DefaultOptions => new AppLeaseOptions();
    }
}
