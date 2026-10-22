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

namespace DurableTask.ServiceBus.Tracking
{
    using System;
    using System.Diagnostics;
    using System.Globalization;
    using System.Text.RegularExpressions;
    using DurableTask.Core.Tracing;
    using DurableTask.Core.Common;

    /// <summary>
    /// A helper class for the Azure blob storage client.
    /// </summary>
    public class BlobStorageClientHelper
    {
        static readonly string DateFormat = "yyyyMMdd";
        static readonly char ContainerNameDelimiter = '-';
        static readonly string ContainerNameMarker = "dtfx";
        static readonly Regex ContainerNameSuffixRegex = new Regex(@"^[a-z0-9-]+\z", RegexOptions.Compiled | RegexOptions.CultureInvariant);

        /// <summary>
        /// the blob storage access key is in the format of {DateTime}|{blobName}
        /// </summary>
        public static readonly char KeyDelimiter = '|';

        /// <summary>
        /// the delimiter shown in the blob name as the file path
        /// </summary>
        public static readonly char BlobNameDelimiter = '/';

        /// <summary>
        /// Build a blob key for the message.
        /// </summary>
        /// <param name="instanceId">The orchestration instance Id</param>
        /// <param name="executionId">The orchestration execution Id</param>
        /// <param name="messageFireTime">The message fire time. If it is DateTime.MinValue, use current date.</param>
        /// <returns>The constructed blob key for message</returns>
        public static string BuildMessageBlobKey(string instanceId, string executionId, DateTime messageFireTime)
        {
            string id = Guid.NewGuid().ToString("N");
            return string.Format(
                "{0}{1}{2}{3}{4}{3}{5}",
                BuildContainerNameSuffix("message", messageFireTime),
                KeyDelimiter,
                instanceId,
                BlobNameDelimiter,
                executionId,
                id);
        }

        static string BuildContainerNameSuffix(string containerType, DateTime blobCreationTime)
        {
            return $"{containerType.ToLowerInvariant()}{ContainerNameDelimiter}{GetDateStringForContainerName(blobCreationTime)}";
        }

        /// <summary>
        /// Build the container name prefix using the lower case hub name.
        /// It is in the format of {hubName}-dtfx.
        /// The container name prefix is not part of the generated blob key.
        /// </summary>
        /// <param name="hubName">The hub name. Converted to lower case to build the prefix.</param>
        /// <returns>The container name prefix</returns>
        public static string BuildContainerNamePrefix(string hubName)
        {
            return $"{hubName.ToLowerInvariant()}{ContainerNameDelimiter}{ContainerNameMarker}";
        }

        /// <summary>
        /// Build a blob key for the session.
        /// </summary>
        /// <param name="sessionId">The session Id</param>
        /// <returns>The constructed blob key for session</returns>
        public static string BuildSessionBlobKey(string sessionId)
        {
            string id = Guid.NewGuid().ToString("N");
            return $"{BuildContainerNameSuffix("session", DateTimeUtils.MinDateTime)}{KeyDelimiter}{sessionId}{BlobNameDelimiter}{id}";
        }

        // use the message fire time if it is set;
        // otherwise, use the current utc time as the date string as part of the container name
        static string GetDateStringForContainerName(DateTime messageFireTime)
        {
            return messageFireTime.IsSet() ?
                messageFireTime.ToString(DateFormat, CultureInfo.InvariantCulture) :
                DateTime.UtcNow.ToString(DateFormat, CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Parse the key for the container name suffix and the blob name.
        /// </summary>
        /// <param name="key">The input blob key</param>
        /// <param name="containerNameSuffix">The parsed container name suffix as output</param>
        /// <param name="blobName">The parsed blob name as output</param>
        public static void ParseKey(string key, out string containerNameSuffix, out string blobName)
        {
            string[] segments = key.Split(new[] { KeyDelimiter }, 2);
            if (segments.Length < 2)
            {
                throw new ArgumentException($"Blob key {key} does not contain required 2 or more segments: containerNameSuffix|blobName.", nameof(key));
            }

            containerNameSuffix = segments[0];
            if (!IsValidContainerNameSuffix(containerNameSuffix))
            {
                throw new ArgumentException(
                    $"Not a valid container name suffix: {containerNameSuffix}. " +
                    "Container name suffix can contain only lower case letters, numbers, and the dash (-) character.",
                    nameof(containerNameSuffix));
            }

            blobName = segments[1];
        }

        /// <summary>
        /// Validate the container name suffix.
        /// Container name suffix can contain only lower case letters, numbers, and the dash (-) character.
        /// </summary>
        /// <param name="containerNameSuffix"></param>
        /// <returns>True if the container name suffix is valid.</returns>
        static bool IsValidContainerNameSuffix(string containerNameSuffix)
        {
            return ContainerNameSuffixRegex.IsMatch(containerNameSuffix);
        }

        /// <summary>
        /// Check if the container is expired.
        /// </summary>
        /// <param name="containerName">The container name</param>
        /// <param name="thresholdDateTimeUtc">The specified date threshold</param>
        /// <returns></returns>
        public static bool IsContainerExpired(string containerName, DateTime thresholdDateTimeUtc)
        {
            // Container names are {hubName}-dtfx-{type}-{yyyyMMdd}; the hub name may itself contain dashes,
            // so the fixed segments are read from the end.
            string[] segments = containerName.Split(ContainerNameDelimiter);
            if (segments.Length < 4 || !string.Equals(segments[segments.Length - 3], ContainerNameMarker, StringComparison.Ordinal))
            {
                TraceHelper.Trace(
                    TraceEventType.Warning,
                    "BlobStorageClientHelper-IsContainerExpired-ContainerIgnored",
                    $"Container name {containerName} is not in the format {{hubName}}-{ContainerNameMarker}-{{type}}-{{date}}. Container {containerName} is ignored.");

                return false;
            }

            string dateString = segments[segments.Length - 1];
            if (!TryParseContainerDate(dateString, out DateTime containerDateTime))
            {
                TraceHelper.Trace(
                    TraceEventType.Warning,
                    "BlobStorageClientHelper-IsContainerExpired-InvalidDate",
                    $"Cannot parse the the date string {dateString} in the format of yyyyMMdd. Container {containerName} is ignored.");

                return false;
            }

            return containerDateTime < thresholdDateTimeUtc;
        }

        /// <summary>
        /// Check if the container belongs to the hub with the given container name prefix.
        /// Container names are {hubName}-dtfx-{type}-{yyyyMMdd}; because hub names may contain dashes, a different hub
        /// (e.g. "alpha-dtfx-backup") can share the prefix "alpha-dtfx". The container belongs to the hub only if the
        /// whole name without its trailing "-{type}-{yyyyMMdd}" segments equals the hub's container name prefix,
        /// the type is not empty, and the last segment is a valid yyyyMMdd date.
        /// </summary>
        /// <param name="containerName">The container name</param>
        /// <param name="containerNamePrefix">The hub's container name prefix, i.e. {hubName}-dtfx</param>
        /// <returns>True if the container belongs to the hub.</returns>
        internal static bool IsContainerOwnedByHub(string containerName, string containerNamePrefix)
        {
            int dateDelimiterIndex = containerName.LastIndexOf(ContainerNameDelimiter);
            if (dateDelimiterIndex <= 0)
            {
                return false;
            }

            int typeDelimiterIndex = containerName.LastIndexOf(ContainerNameDelimiter, dateDelimiterIndex - 1);
            if (typeDelimiterIndex <= 0 || typeDelimiterIndex == dateDelimiterIndex - 1)
            {
                return false;
            }

            return string.Equals(containerName.Substring(0, typeDelimiterIndex), containerNamePrefix, StringComparison.OrdinalIgnoreCase) &&
                TryParseContainerDate(containerName.Substring(dateDelimiterIndex + 1), out _);
        }

        static bool TryParseContainerDate(string dateString, out DateTime date)
        {
            return DateTime.TryParseExact(dateString, DateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out date);
        }

        /// <summary>
        /// Build a container name using prefix and suffix.
        /// </summary>
        /// <param name="prefix">The container name prefix</param>
        /// <param name="suffix">The container name suffix</param>
        /// <returns>The container name</returns>
        public static string BuildContainerName(string prefix, string suffix)
        {
            return $"{prefix}{ContainerNameDelimiter}{suffix}";
        }
    }
}
