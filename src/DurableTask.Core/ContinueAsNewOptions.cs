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
namespace DurableTask.Core
{
    /// <summary>
    /// Configures how an orchestration continues as new.
    /// </summary>
    public sealed class ContinueAsNewOptions
    {
        /// <summary>
        /// Gets or sets how distributed tracing should behave for the next generation.
        /// The default is <see cref="ContinueAsNewTraceBehavior.PreserveTraceContext"/>,
        /// which keeps the next generation in the same distributed trace.
        /// </summary>
        public ContinueAsNewTraceBehavior TraceBehavior { get; set; } =
            ContinueAsNewTraceBehavior.PreserveTraceContext;
    }

    /// <summary>
    /// Describes how distributed tracing should behave for the next <c>ContinueAsNew</c> generation.
    /// </summary>
    public enum ContinueAsNewTraceBehavior
    {
        /// <summary>
        /// Preserve the current trace lineage across generations. This is the default.
        /// </summary>
        PreserveTraceContext = 0,

        /// <summary>
        /// Start the next generation in a fresh distributed trace. Useful for long-running
        /// periodic orchestrations where each cycle should be independently observable.
        /// </summary>
        StartNewTrace = 1,
    }
}
