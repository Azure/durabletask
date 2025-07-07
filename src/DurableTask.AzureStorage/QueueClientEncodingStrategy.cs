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

using Azure.Storage.Queues;

namespace DurableTask.AzureStorage
{
    /// <summary>
    /// Specifies the encoding strategy used for Azure Storage Queue messages.
    /// This enum maps to the Azure Storage SDK's <see cref="QueueMessageEncoding"/> values.
    /// </summary>
    public enum QueueClientMessageEncoding
    {
        /// <summary>
        /// Use UTF8 encoding for queue messages. Maps to <see cref="QueueMessageEncoding.None"/>.
        /// </summary>
        UTF8 = 0,

        /// <summary>
        /// Use Base64 encoding for queue messages. Maps to <see cref="QueueMessageEncoding.Base64"/>.
        /// This provides compatibility with older versions and may be required in some scenarios 
        /// where UTF8 encoding is not supported.
        /// </summary>
        Base64 = 1,
    }
}
