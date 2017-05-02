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
    using System;
    using DurableTask.Core;

    /// <summary>
    ///     Settings to configure the Jumpstart manager
    /// </summary>
    public class JumpStartSettings
    {
        internal JumpStartSettings()
        {
            this.JumpStartEnabled = true;
            this.Interval = FrameworkConstants.JumpStartDefaultInterval;
            this.IgnoreWindow = FrameworkConstants.JumpStartDefaultIgnoreWindow;
        }

        /// <summary>
        ///     Boolean indicating whether to enable the jumpstart manager or not.
        /// </summary>
        public bool JumpStartEnabled { get; set; }

        /// <summary>
        ///     Time frequency for the jumpstart manager to poll
        /// </summary>
        public TimeSpan Interval { get; set; }

        /// <summary>
        ///     Window of time to ignore when polling to allow messages to process
        /// </summary>
        public TimeSpan IgnoreWindow { get; set; }
    }
}
