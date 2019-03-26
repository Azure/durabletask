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

namespace DurableTask.Redis
{
    using System;

    /// <summary>
    /// Settings for the <see cref="RedisOrchestrationService"/> class.
    /// </summary>
    public class RedisOrchestrationServiceSettings
    {
        /// <summary>
        /// Gets or sets the name of the task hub. This value is used to group related resources.
        /// All Redis Keys in this TaskHub will be prefixed with "{TaskHubName}."
        /// </summary>
        public string TaskHubName { get; set; }

        /// <summary>
        /// Gets or sets a redis connection string.
        /// </summary>
        /// <remarks>
        /// This connection string is for use with the <c>StackExchange.Redis</c> client library.
        /// For documentation on supported values, see https://stackexchange.github.io/StackExchange.Redis/Configuration.
        /// </remarks>
        public string RedisConnectionString { get; set; } = "localhost";

        /// <summary>
        /// Gets or sets the maximum number of work items that can be processed concurrently on a single node.
        /// The default value is 100.
        /// </summary>
        public int MaxConcurrentTaskActivityWorkItems { get; set; } = 100;

        /// <summary>
        /// Gets or sets the maximum number of orchestrations that can be processed concurrently on a single node.
        /// The default value is 100.
        /// </summary>
        public int MaxConcurrentTaskOrchestrationWorkItems { get; set; } = 100;

        /// <summary>
        /// Validates the specified <see cref="RedisOrchestrationServiceSettings"/> object.
        /// </summary>
        /// <param name="settings">The <see cref="RedisOrchestrationServiceSettings"/> object to validate.</param>
        /// <returns>Returns <paramref name="settings"/> if successfully validated.</returns>
        public static RedisOrchestrationServiceSettings Validate(RedisOrchestrationServiceSettings settings)
        {
            if (settings == null)
            {
                throw new ArgumentNullException(nameof(settings));
            }

            if (string.IsNullOrEmpty(settings.RedisConnectionString))
            {
                throw new ArgumentNullException(nameof(settings.RedisConnectionString));
            }

            if (settings.MaxConcurrentTaskOrchestrationWorkItems <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(settings.MaxConcurrentTaskOrchestrationWorkItems));
            }

            if (settings.MaxConcurrentTaskActivityWorkItems <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(settings.MaxConcurrentTaskActivityWorkItems));
            }

            return settings;
        }
    }
}
