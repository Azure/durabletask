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

#nullable enable
using System;

namespace DurableTask.Core.Serializing.Internal
{
    /// <summary>
    /// This is an internal API that supports the DurableTask infrastructure and not subject to the same compatibility
    /// standards as public APIs. It may be changed or removed without notice in any release. You should only use it
    /// directly in your code with extreme caution and knowing that doing so can result in application failures when
    /// updating to a new DurableTask release.
    /// </summary>
    [Obsolete("Not for public consumption.")]
    public sealed class RawInput
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RawInput" /> class.
        /// </summary>
        /// <param name="value">The raw input value to use.</param>
        public RawInput(string? value)
        {
            this.Value = value;
        }

        /// <summary>
        /// Gets the raw input value.
        /// </summary>
        public string? Value { get; }
    }
}
