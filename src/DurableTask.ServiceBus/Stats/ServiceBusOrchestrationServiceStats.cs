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

namespace DurableTask.ServiceBus.Stats
{
    /// <summary>
    /// Stats for the ServiceBusOrchestrationService
    /// </summary>
    public class ServiceBusOrchestrationServiceStats
    {
        // TODO: This should implement an interface so implementation specific perf counter implementations are possible
        // TODO: Add: 
        //      receive latency
        //      message/session throughput
        //      average E2E latency(i.e.look at curenttimeutc -enqueuedtimeutc on the received message)
        //      queue depths

        /// <summary>
        /// Creates a new instance of the ServiceBusOrchestrationServiceStats class
        /// </summary>
        public ServiceBusOrchestrationServiceStats()
        {
            this.OrchestrationDispatcherStats = new ServiceBusQueueStats();
            this.ActivityDispatcherStats = new ServiceBusQueueStats();
            this.TrackingDispatcherStats = new ServiceBusQueueStats();
        }

        /// <summary>
        /// The orchestration dispatcher queue stats
        /// </summary>
        public ServiceBusQueueStats OrchestrationDispatcherStats { get; private set; }

        /// <summary>
        /// The activity dispatcher queue stats
        /// </summary>
        public ServiceBusQueueStats ActivityDispatcherStats { get; private set; }

        /// <summary>
        /// The tracking dispatcher queue stats
        /// </summary>
        public ServiceBusQueueStats TrackingDispatcherStats { get; private set; }

        /// <summary>
        /// Returns a string that represents the stats for each queue.
        /// </summary>
        public override string ToString()
        {
            return $"OrchestrationDispatcherStats: {{{OrchestrationDispatcherStats}}}"
                   + $", ActivityDispatcherStats: {{{ActivityDispatcherStats}}}"
                   + $", TrackingDispatcherStats: {{{TrackingDispatcherStats}}}";
        }
    }
}
