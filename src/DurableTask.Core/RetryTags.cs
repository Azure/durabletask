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
    /// <summary>
    /// Reserved tag keys written by DTFx into <see cref="ScheduleTaskOptions.Tags"/> (and from there into
    /// <see cref="History.TaskScheduledEvent.Tags"/>) when an activity is scheduled under a retry policy.
    /// </summary>
    /// <remarks>
    /// The <c>dt.</c> prefix is reserved for DTFx-injected metadata; customers must not use it on
    /// caller-supplied tags.
    /// </remarks>
    internal static class RetryTags
    {
        /// <summary>
        /// 1-based attempt counter for the current schedule attempt. Decimal string in
        /// <see cref="System.Globalization.CultureInfo.InvariantCulture"/>.
        /// </summary>
        public const string Attempt = "dt.retry.attempt";

        /// <summary>
        /// Policy ceiling — the <see cref="RetryOptions.MaxNumberOfAttempts"/> value from the
        /// retry policy in effect for this schedule. Decimal string in
        /// <see cref="System.Globalization.CultureInfo.InvariantCulture"/>.
        /// </summary>
        public const string MaxAttempts = "dt.retry.maxAttempts";
    }
}
