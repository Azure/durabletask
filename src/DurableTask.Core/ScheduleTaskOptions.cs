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
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Options for scheduling a task.
    /// </summary>
    public class ScheduleTaskOptions
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ScheduleTaskOptions"/> class.
        /// </summary>
        protected ScheduleTaskOptions()
        {
        }

        /// <summary>
        /// Dictionary of key/value tags associated with this instance.
        /// </summary>
        public IDictionary<string, string>? Tags { get; internal set; }

        /// <summary>
        /// Gets or sets the retry options for the scheduled task.
        /// </summary>
        public RetryOptions? RetryOptions { get; internal set; }

        /// <summary>
        /// Creates a new builder for constructing a <see cref="ScheduleTaskOptions"/> instance.
        /// </summary>
        /// <returns>A new builder for creating schedule task options.</returns>
        public static Builder CreateBuilder()
        {
            return new Builder();
        }

        /// <summary>
        /// Builder class for creating instances of <see cref="ScheduleTaskOptions"/>.
        /// </summary>
        public class Builder
        {
            private readonly ScheduleTaskOptions options;

            /// <summary>
            /// Initializes a new instance of the <see cref="Builder"/> class.
            /// </summary>
            internal Builder()
            {
                this.options = new ScheduleTaskOptions();
            }

            /// <summary>
            /// Sets the tags for the schedule task options.
            /// </summary>
            /// <param name="tags">The dictionary of key/value tags.</param>
            /// <returns>The builder instance.</returns>
            public Builder WithTags(IDictionary<string, string> tags)
            {
                this.options.Tags = new Dictionary<string, string>(tags);
                return this;
            }

            /// <summary>
            /// Adds a tag to the schedule task options.
            /// </summary>
            /// <param name="key">The tag key.</param>
            /// <param name="value">The tag value.</param>
            /// <returns>The builder instance.</returns>
            public Builder AddTag(string key, string value)
            {
                if (this.options.Tags == null)
                {
                    this.options.Tags = new Dictionary<string, string>();
                }

                this.options.Tags[key] = value;
                return this;
            }

            /// <summary>
            /// Sets the retry options for the scheduled task.
            /// </summary>
            /// <param name="retryOptions">The retry options to use.</param>
            /// <returns>The builder instance.</returns>
            public Builder WithRetryOptions(RetryOptions retryOptions)
            {
                this.options.RetryOptions = retryOptions == null ? null : new RetryOptions(retryOptions);
                return this;
            }

            /// <summary>
            /// Sets the retry options for the scheduled task with the specified parameters.
            /// </summary>
            /// <param name="firstRetryInterval">Timespan to wait for the first retry.</param>
            /// <param name="maxNumberOfAttempts">Max number of attempts to retry.</param>
            /// <returns>The builder instance.</returns>
            public Builder WithRetryOptions(TimeSpan firstRetryInterval, int maxNumberOfAttempts)
            {
                this.options.RetryOptions = new RetryOptions(firstRetryInterval, maxNumberOfAttempts);
                return this;
            }

            /// <summary>
            /// Sets the retry options for the scheduled task with the specified parameters and configures additional properties.
            /// </summary>
            /// <param name="firstRetryInterval">Timespan to wait for the first retry.</param>
            /// <param name="maxNumberOfAttempts">Max number of attempts to retry.</param>
            /// <param name="configureRetryOptions">Action to configure additional retry option properties.</param>
            /// <returns>The builder instance.</returns>
            public Builder WithRetryOptions(TimeSpan firstRetryInterval, int maxNumberOfAttempts, Action<RetryOptions> configureRetryOptions)
            {
                var retryOptions = new RetryOptions(firstRetryInterval, maxNumberOfAttempts);
                configureRetryOptions?.Invoke(retryOptions);
                this.options.RetryOptions = retryOptions;
                return this;
            }

            /// <summary>
            /// Builds the <see cref="ScheduleTaskOptions"/> instance.
            /// </summary>
            /// <returns>The built schedule task options.</returns>
            public ScheduleTaskOptions Build()
            {
                return this.options;
            }
        }
    }
}