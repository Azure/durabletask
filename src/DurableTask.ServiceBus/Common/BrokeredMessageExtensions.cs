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

namespace DurableTask.ServiceBus.Common.Abstraction
{
    using System;
    using DurableTask.Core.Common;

    /// <summary>
    /// Extension methods for BrokeredMessage
    /// </summary>
    public static class BrokeredMessageExtensions
    {
        /// <summary>
        /// Returns delivery latency of the message
        /// </summary>        
        public static double DeliveryLatency(this Message message)
        {
            if (message == null)
            {
                return 0;
            }

            DateTime actualEnqueueTimeUtc = (!message.ScheduledEnqueueTimeUtc.IsSet()) ? message.SystemProperties.EnqueuedTimeUtc : message.ScheduledEnqueueTimeUtc;
            return (DateTime.UtcNow - actualEnqueueTimeUtc).TotalMilliseconds;
        }
    }
}
