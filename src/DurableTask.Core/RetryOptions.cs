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

namespace DurableTask.Core
{
    using System;

    /// <summary>
    ///     Contains retry policies that can be passed as parameters to various operations
    /// </summary>
    public class RetryOptions
    {
        /// <summary>
        /// Creates a new instance RetryOptions with the supplied first retry and max attempts
        /// </summary>
        /// <param name="firstRetryInterval">Timespan to wait for the first retry</param>
        /// <param name="maxNumberOfAttempts">Max number of attempts to retry</param>
        /// <exception cref="ArgumentException"></exception>
        public RetryOptions(TimeSpan firstRetryInterval, int maxNumberOfAttempts)
        {
            if (firstRetryInterval <= TimeSpan.Zero)
            {
                throw new ArgumentException("Invalid interval.  Specify a TimeSpan value greater then TimeSpan.Zero.",
                    nameof(firstRetryInterval));
            }

            FirstRetryInterval = firstRetryInterval;
            MaxNumberOfAttempts = maxNumberOfAttempts;
            // Defaults
            MaxRetryInterval = TimeSpan.MaxValue;
            BackoffCoefficient = 1;
            RetryTimeout = TimeSpan.MaxValue;
            Handle = e => true;
        }

        /// <summary>
        /// Creates a new instance of RetryOptions by copying values from an existing instance
        /// </summary>
        /// <param name="source">The RetryOptions instance to copy from</param>
        /// <exception cref="ArgumentNullException"></exception>
        public RetryOptions(RetryOptions source)
        {
            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            FirstRetryInterval = source.FirstRetryInterval;
            MaxNumberOfAttempts = source.MaxNumberOfAttempts;
            MaxRetryInterval = source.MaxRetryInterval;
            BackoffCoefficient = source.BackoffCoefficient;
            RetryTimeout = source.RetryTimeout;
            Handle = source.Handle;
        }

        /// <summary>
        /// Gets or sets the first retry interval
        /// </summary>
        public TimeSpan FirstRetryInterval { get; set; }

        /// <summary>
        /// Gets or sets the max retry interval
        /// defaults to TimeSpan.MaxValue
        /// </summary>
        public TimeSpan MaxRetryInterval { get; set; }

        /// <summary>
        /// Gets or sets the back-off coefficient
        /// defaults to 1, used to determine rate of increase of back-off
        /// </summary>
        // ReSharper disable once IdentifierTypo (avoid breaking change)
        public double BackoffCoefficient { get; set; }

        /// <summary>
        /// Gets or sets the timeout for retries
        /// defaults to TimeSpan.MaxValue
        /// </summary>
        public TimeSpan RetryTimeout { get; set; }

        /// <summary>
        /// Gets or sets the max number of attempts
        /// </summary>
        public int MaxNumberOfAttempts { get; set; }

        /// <summary>
        /// Gets or sets a Func to call on exception to determine if retries should proceed
        /// </summary>
        public Func<Exception, bool> Handle { get; set; }
    }
}