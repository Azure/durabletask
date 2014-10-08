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
    using System;

    /// <summary>
    ///     Contains retry policies that can be passed as parameters to various operations
    /// </summary>
    public class RetryOptions
    {
        public RetryOptions(TimeSpan firstRetryInterval, int maxNumberOfAttempts)
        {
            if (firstRetryInterval <= TimeSpan.Zero)
            {
                throw new ArgumentException("Invalid interval.  Specify a TimeSpan value greater then TimeSpan.Zero.",
                    "firstRetryInterval");
            }

            FirstRetryInterval = firstRetryInterval;
            MaxNumberOfAttempts = maxNumberOfAttempts;
            // Defaults
            MaxRetryInterval = TimeSpan.MaxValue;
            BackoffCoefficient = 1;
            RetryTimeout = TimeSpan.MaxValue;
            Handle = e => true;
        }

        public TimeSpan FirstRetryInterval { get; set; }
        public TimeSpan MaxRetryInterval { get; set; }
        public double BackoffCoefficient { get; set; }
        public TimeSpan RetryTimeout { get; set; }
        public int MaxNumberOfAttempts { get; set; }
        public Func<Exception, bool> Handle { get; set; }
    }
}