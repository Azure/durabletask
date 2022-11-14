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
namespace DurableTask.AzureStorage.Tracking
{
    using System.Collections.Generic;

    /// <summary>
    /// Represents an OData condition as specified by a filter and a projection.
    /// </summary>
    public readonly struct ODataCondition
    {
        /// <summary>
        /// Gets the optional collection of properties to be projected.
        /// </summary>
        /// <value>An optional enumerable of zero or more property names.</value>
        public IEnumerable<string>? Select { get; }

        /// <summary>
        /// Gets the optional filter expression.
        /// </summary>
        /// <value>An optional string that represents the filter expression.</value>
        public string? Filter { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="ODataCondition"/> structure based on the specified values.
        /// </summary>
        /// <param name="select">An optional collection of properties to be projected.</param>
        /// <param name="filter">An optional filter expression.</param>
        public ODataCondition(IEnumerable<string>? select = null, string? filter = null)
        {
            Select = select;
            Filter = filter;
        }
    }
}
