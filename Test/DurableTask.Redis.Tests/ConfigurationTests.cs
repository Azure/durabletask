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

namespace DurableTask.Redis.Tests
{
    using System;
    using Xunit;

    public class ConfigurationTests
    {
        /// <summary>
        /// Ensures default settings are valid.
        /// </summary>
        [Fact]
        public void ValidateDefaultSettings()
        {
            RedisOrchestrationServiceSettings.Validate(
                new RedisOrchestrationServiceSettings());
        }

        /// <summary>
        /// Ensures null settings are invalid.
        /// </summary>
        [Fact]
        public void ValidateNullSettings()
        {
            Assert.Throws<ArgumentNullException>(
                () => RedisOrchestrationServiceSettings.Validate(null));
        }

        /// <summary>
        /// Ensures settings with null connection strings are invalid.
        /// </summary>
        [Fact]
        public void ValidateNullConnectionString()
        {
            Assert.Throws<ArgumentNullException>(
                () => RedisOrchestrationServiceSettings.Validate(
                    new RedisOrchestrationServiceSettings
                    {
                        RedisConnectionString = null,
                    }));

            Assert.Throws<ArgumentNullException>(
                () => RedisOrchestrationServiceSettings.Validate(
                    new RedisOrchestrationServiceSettings
                    {
                        RedisConnectionString = string.Empty,
                    }));
        }

        /// <summary>
        /// Ensures settings with non-positive work item concurrencies are invalid.
        /// </summary>
        [Fact]
        public void ValidateNonPositiveWorkItemConcurrency()
        {
            Assert.Throws<ArgumentOutOfRangeException>(
                () => RedisOrchestrationServiceSettings.Validate(
                    new RedisOrchestrationServiceSettings
                    {
                         MaxConcurrentTaskActivityWorkItems = 0,
                    }));

            Assert.Throws<ArgumentOutOfRangeException>(
                () => RedisOrchestrationServiceSettings.Validate(
                    new RedisOrchestrationServiceSettings
                    {
                        MaxConcurrentTaskOrchestrationWorkItems = 0,
                    }));

            Assert.Throws<ArgumentOutOfRangeException>(
                () => RedisOrchestrationServiceSettings.Validate(
                    new RedisOrchestrationServiceSettings
                    {
                        MaxConcurrentTaskActivityWorkItems = -1,
                    }));

            Assert.Throws<ArgumentOutOfRangeException>(
                () => RedisOrchestrationServiceSettings.Validate(
                    new RedisOrchestrationServiceSettings
                    {
                        MaxConcurrentTaskOrchestrationWorkItems = -1,
                    }));
        }
    }
}
