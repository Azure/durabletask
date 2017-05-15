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

namespace DurableTask.ServiceBus.Settings
{
    using DurableTask.Core;
    using DurableTask.Core.Settings;

    /// <summary>
    ///     Settings to configure the Service Bus session.
    /// </summary>
    public class ServiceBusSessionSettings : ISessionSettings
    {
        internal ServiceBusSessionSettings() :
            this (FrameworkConstants.SessionOverflowThresholdInBytesDefault, FrameworkConstants.SessionMaxSizeInBytesDefault)
        {
        }

        internal ServiceBusSessionSettings(int sessionOverflowThresholdInBytes, int sessionMaxSizeInBytes)
        {
            SessionOverflowThresholdInBytes = sessionOverflowThresholdInBytes;
            SessionMaxSizeInBytes = sessionMaxSizeInBytes;
        }

        /// <summary>
        ///     The max allowed session size in service bus. Default is 230K.
        /// </summary>
        public int SessionOverflowThresholdInBytes { get; set; }

        /// <summary>
        ///     The max allowed session size for external storage. Default is 10M.
        /// </summary>
        public int SessionMaxSizeInBytes { get; set; }
    }
}
