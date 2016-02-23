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

namespace DurableTask
{
    using Common;

    /// <summary>
    ///     Configuration for various TaskHubClient options
    /// </summary>
    public sealed class TaskHubClientSettings
    {
        /// <summary>
        ///     Create a TaskHubClientSettings object with default settings
        /// </summary>
        public TaskHubClientSettings()
        {
            MessageCompressionSettings = new CompressionSettings
            {
                Style = CompressionStyle.Never,
                ThresholdInBytes = 0
            };
        }

        public CompressionSettings MessageCompressionSettings { get; set; }

        internal TaskHubClientSettings Clone()
        {
            var clonedSettings = new TaskHubClientSettings();
            clonedSettings.MessageCompressionSettings = MessageCompressionSettings;
            return clonedSettings;
        }
    }
}