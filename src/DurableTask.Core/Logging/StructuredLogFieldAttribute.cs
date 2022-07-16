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

namespace DurableTask.Core.Logging
{
    using System;

    /// <summary>
    /// Attribute used to indicate that a <see cref="ILogEvent"/> field should be serialized to the structured log provider.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class StructuredLogFieldAttribute : Attribute
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StructuredLogFieldAttribute"/> class.
        /// </summary>
        /// <param name="name">The name of the log entry field. If not specified, the name of the property is used.</param>
        public StructuredLogFieldAttribute(string name = null)
        {
            this.Name = name;
        }

        /// <summary>
        /// Gets the name of the log entry field. If not specified, the name of the property is used.
        /// </summary>
        public string Name { get; }
    }
}
