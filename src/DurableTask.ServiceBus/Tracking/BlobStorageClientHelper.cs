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

        /// <summary>
        /// the blob storage accesss key is in the format of {DateTime}|{blobName}
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
            return $"{containerType.ToLower()}{ContainerNameDelimiter}{GetDateStringForContainerName(blobCreationTime)}";
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
            return $"{hubName.ToLower()}{ContainerNameDelimiter}dtfx";
        }

        /// <summary>
        /// Build a blob key for the session.
        /// </summary>
        /// <param name="sessionId">The session Id</param>
        /// <returns>The constructed blob key for session</returns>
        public static string BuildSessionBlobKey(string sessionId)
        {
            string id = Guid.NewGuid().ToString("N");
            return string.Format(
                "{0}{1}{2}{3}{4}",
                BuildContainerNameSuffix("session", DateTimeUtils.MinDateTime),
                KeyDelimiter,
                sessionId,
                BlobNameDelimiter,
                id);
        }

        // use the message fire time if it is set;
        // otherwise, use the current utc time as the date string as part of the container name
        static string GetDateStringForContainerName(DateTime messageFireTime)
        {
            return messageFireTime.IsSet() ?
                messageFireTime.ToString(DateFormat) :
                DateTime.UtcNow.ToString(DateFormat);
        }

        /// <summary>
        /// Parse the key for the container name suffix and the blob name.
        /// </summary>
        /// <param name="key">The input blob key</param>
        /// <param name="containerNameSuffix">The parsed container name suffix as output</param>
        /// <param name="blobName">The parsed blob name as output</param>
        public static void ParseKey(string key, out string containerNameSuffix, out string blobName)
        {
            string[] segments = key.Split(new[] {BlobStorageClientHelper.KeyDelimiter}, 2);
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
            Regex regex = new Regex(@"^[a-z0-9\\-]+$");
            return regex.Match(containerNameSuffix).Success;
        }

        /// <summary>
        /// Check if the container is expired.
        /// </summary>
        /// <param name="containerName">The container name</param>
        /// <param name="thresholdDateTimeUtc">The specified date threshold</param>
        /// <returns></returns>
        public static bool IsContainerExpired(string containerName, DateTime thresholdDateTimeUtc)
        {
            string[] segments = containerName.Split(ContainerNameDelimiter);
            if (segments.Length != 4)
            {
                TraceHelper.Trace(
                    TraceEventType.Warning,
                    "BlobStorageClientHelper-IsContainerExpired-ContainerIgnored",
                    $"Container name {containerName} does not contain required 4 segments. Container {containerName} is ignored.");

                return false;
            }

            DateTime containerDateTime;
            string dateString = segments[segments.Length - 1];
            bool parseSucceeded = DateTime.TryParseExact(
                dateString,
                DateFormat,
                System.Globalization.CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out containerDateTime);

            if (!parseSucceeded)
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
