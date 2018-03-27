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

namespace DurableTask.Core.Tracking
{
    using System;
    using System.IO;
    using System.Threading.Tasks;

    /// <summary>
    /// Interface to allow save and load large blobs, such as message and session, as a stream using a storage store.
    /// The blob is saved in the store using an access key (e.g., a path to the blob),
    /// which can be used to uniquely load the blob back.
    /// </summary>
    public interface IOrchestrationServiceBlobStore
    {
        /// <summary>
        /// Create a blob storage access key based on the orchestrationInstance.
        /// This key will be used to save and load the stream message in external storage when it is too large.
        /// </summary>
        /// <param name="orchestrationInstance">The orchestration instance.</param>
        /// <param name="messageFireTime">The message fire time. Could be DateTime.MinValue.</param>
        /// <returns>A message blob key.</returns>
        string BuildMessageBlobKey(OrchestrationInstance orchestrationInstance, DateTime messageFireTime);

        /// <summary>
        /// Create a blob storage access key based on message session.
        /// This key will be used to save and load the stream in external storage when it is too large.
        /// </summary>
        /// <param name="sessionId">The message session Id.</param>
        /// <returns>A blob key.</returns>
        string BuildSessionBlobKey(string sessionId);

        /// <summary>
        /// Save the stream of the message or seesion using key.
        /// </summary>
        /// <param name="blobKey">The blob key.</param>
        /// <param name="stream">The stream of the message or session.</param>
        /// <returns></returns>
        Task SaveStreamAsync(string blobKey, Stream stream);

        /// <summary>
        /// Load the stream of message or seesion from storage using key.
        /// </summary>
        /// <param name="blobKey">The blob key.</param>
        /// <returns>The saved stream message or session.</returns>
        Task<Stream> LoadStreamAsync(string blobKey);

        /// <summary>
        /// Deletes the blob store
        /// </summary>
        Task DeleteStoreAsync();

        /// <summary>
        /// Purges expired containers from storage for given time threshold
        /// </summary>
        /// <param name="thresholdDateTimeUtc">The datetime in UTC to use as the threshold for purging containers</param>
        Task PurgeExpiredBlobsAsync(DateTime thresholdDateTimeUtc);
    }
}
