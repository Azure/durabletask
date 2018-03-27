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

namespace DurableTask.Core.Settings
{
    using DurableTask.Core.Common;

    /// <summary>
    /// Compression settings
    /// </summary>
    public struct CompressionSettings
    {
        /// <summary>
        ///     Type of compression
        /// </summary>
        public CompressionStyle Style { get; set; }

        /// <summary>
        ///     Compression threshold in bytes; if specified by compression criteria, compression will not be done
        ///     if size is below this value
        /// </summary>
        public int ThresholdInBytes { get; set; }
    }
}