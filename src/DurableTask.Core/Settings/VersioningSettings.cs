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

using System;

namespace DurableTask.Core.Settings
{
    /// <summary>
    /// Collection of settings that define the overall versioning behavior.
    /// </summary>
    public class VersioningSettings
    {
        /// <summary>
        /// Defines the version matching strategy for the Durable Task worker.
        /// </summary>
        public enum VersionMatchStrategy
        {
            /// <summary>
            /// Ignore Orchestration version, all work received is processed.
            /// </summary>
            None = 0,

            /// <summary>
            /// Worker will only process Tasks from Orchestrations with the same version as the worker.
            /// </summary>
            Strict = 1,

            /// <summary>
            /// Worker will process Tasks from Orchestrations whose version is less than or equal to the worker.
            /// </summary>
            CurrentOrOlder = 2,
        }

        /// <summary>
        /// Defines the versioning failure strategy for the Durable Task worker.
        /// </summary>
        public enum VersionFailureStrategy
        {
            /// <summary>
            /// Do not change the orchestration state if the version does not adhere to the matching strategy.
            /// </summary>
            Reject = 0,

            /// <summary>
            /// Fail the orchestration if the version does not adhere to the matching strategy.
            /// </summary>
            Fail = 1,
        }

        /// <summary>
        /// Gets or sets the version associated with the settings.
        /// </summary>
        public string Version { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the <see cref="VersionMatchStrategy"/> that is used for matching versions.
        /// </summary>
        public VersionMatchStrategy MatchStrategy { get; set; } = VersionMatchStrategy.None;

        /// <summary>
        /// Gets or sets the <see cref="VersionFailureStrategy"/> that is used to determine what happens on a versioning failure.
        /// </summary>
        public VersionFailureStrategy FailureStrategy { get; set; } = VersionFailureStrategy.Reject;

        /// <summary>
        /// Compare two versions to each other.
        /// </summary>
        /// <remarks>
        /// This method's comparison is handled in the following order:
        ///   1. The versions are checked if they are empty (non-versioned). Both being empty signifies equality.
        ///   2. If sourceVersion is empty but otherVersion is defined, this is treated as the source being less than the other.
        ///   3. If otherVersion is empty but sourceVersion is defined, this is treated as the source being greater than the other.
        ///   4. Both versions are attempted to be parsed into System.Version and compared as such.
        ///   5. If all else fails, a direct string comparison is done between the versions.
        /// </remarks>
        /// <param name="sourceVersion">The source version that will be compared against the other version.</param>
        /// <param name="otherVersion">The other version to compare against.</param>
        /// <returns>An int representing how sourceVersion compares to otherVersion.</returns>
        public static int CompareVersions(string sourceVersion, string otherVersion)
        {
            // Both versions are empty, treat as equal.
            if (string.IsNullOrWhiteSpace(sourceVersion) && string.IsNullOrWhiteSpace(otherVersion))
            {
                return 0;
            }

            // An empty version in the context is always less than a defined version in the parameter.
            if (string.IsNullOrWhiteSpace(sourceVersion))
            {
                return -1;
            }

            // An empty version in the parameter is always less than a defined version in the context.
            if (string.IsNullOrWhiteSpace(otherVersion))
            {
                return 1;
            }

            // If both versions use the .NET Version class, return that comparison.
            if (System.Version.TryParse(sourceVersion, out Version parsedSourceVersion) && System.Version.TryParse(otherVersion, out Version parsedOtherVersion))
            {
                return parsedSourceVersion.CompareTo(parsedOtherVersion);
            }

            // If we have gotten to here, we don't know the syntax of the versions we are comparing, use a string comparison as a final check.
            return string.Compare(sourceVersion, otherVersion, StringComparison.OrdinalIgnoreCase);
        }
    }
}
