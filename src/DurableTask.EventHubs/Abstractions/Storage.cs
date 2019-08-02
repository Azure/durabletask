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
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.EventHubs
{
    /// <summary>
    /// Abstractions for the storage, that allow different providers to be used.
    /// </summary>
    internal static class Storage
    {
        /// <summary>
        /// The event-sourced state of a partition, suitable for asynchronous checkpointing
        /// </summary>
        internal interface IPartitionState
        {
            // ------ intialization ------

            Task<long> RestoreAsync(Partition localPartition);

            Task ShutdownAsync();

            // ------ methods called from updater thread ------

            void Process(PartitionEvent evt);

            // ------ methods called from any thread ------

            Task<TResult> ReadAsync<TResult>(Func<TResult> read);

            Task<TResult> ReadAsync<TArgument1, TResult>(Func<TArgument1, TResult> read, TArgument1 argument);

            // ------ tracked objects ------

            ClocksState Clocks { get; }

            ReassemblyState Reassembly { get; }

            OutboxState Outbox { get; }

            TimersState Timers { get; }

            ActivitiesState Activities { get; }

            SessionsState Sessions { get; }

            InstanceState GetInstance(string instanceId);

            HistoryState GetHistory(string instanceId);
        }
    }
}