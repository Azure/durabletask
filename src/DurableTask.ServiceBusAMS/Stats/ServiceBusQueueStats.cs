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
    using DurableTask.Core.Stats;

    /// <summary>
    /// Stats container for a Service Bus queue
    /// </summary>
    public class ServiceBusQueueStats
    {
        /// <summary>
        /// The number of actual messages sent
        /// </summary>
        public Counter MessagesSent { get; } = new Counter();

        /// <summary>
        /// The number of actual messages received
        /// </summary>
        public Counter MessagesReceived { get; } = new Counter();

        /// <summary>
        /// The number of batches sent
        /// </summary>
        public Counter MessageBatchesSent { get; } = new Counter();

        /// <summary>
        /// The number of batches received
        /// </summary>
        public Counter MessageBatchesReceived { get; } = new Counter();

        /// <summary>
        /// The number of messages renewed
        /// </summary>
        public Counter SessionsRenewed { get; } = new Counter();

        /// <summary>
        /// The number of sessions received
        /// </summary>
        public Counter SessionsReceived { get; } = new Counter();

        /// <summary>
        /// The number of session batches completed
        /// </summary>
        public Counter SessionBatchesCompleted { get; } = new Counter();

        /// <summary>
        /// The number of session state sets
        /// </summary>
        public Counter SessionSets { get; } = new Counter();

        /// <summary>
        /// The number of session state gets
        /// </summary>
        public Counter SessionGets { get; } = new Counter();

        /// <summary>
        /// Returns a string that represents the Queue Stats.
        /// </summary>
        public override string ToString()
        {
            return $"MessagesSent: {MessagesSent}"
                   + $", MessagesReceived: {MessagesReceived}"
                   + $", MessageBatchesSent: {MessageBatchesSent}"
                   + $", MessageBatchesReceived: {MessageBatchesReceived}"
                   + $", SessionsRenewed: {SessionsRenewed}"
                   + $", SessionsReceived: {SessionsReceived}"
                   + $", SessionBatchesCompleted: {SessionBatchesCompleted}"
                   + $", SessionSets: {SessionSets}"
                   + $", SessionGets: {SessionGets}";
        }
    }
}
