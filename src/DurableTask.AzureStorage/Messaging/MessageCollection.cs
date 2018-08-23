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

namespace DurableTask.AzureStorage.Messaging
{
    using System.Collections;
    using System.Collections.Generic;
    using Microsoft.WindowsAzure.Storage.Queue;

    class MessageCollection : IReadOnlyList<MessageData>
    {
        readonly List<MessageData> innerList = new List<MessageData>();

        public MessageData this[int index] => this.innerList[index];

        public int Count => this.innerList.Count;

        /// <summary>
        /// Adds or replaces a message in the list based on the message ID. Returns true for adds, false for replaces.
        /// </summary>
        public bool AddOrReplace(MessageData message)
        {
            // If a message has been sitting in the buffer for too long, the invisibility timeout may expire and 
            // it may get dequeued a second time. In such cases, we should replace the existing copy of the message
            // with the newer copy to ensure it can be deleted successfully after being processed.
            for (int i = 0; i < this.innerList.Count; i++)
            {
                CloudQueueMessage existingMessage = this.innerList[i].OriginalQueueMessage;
                if (existingMessage.Id == message.OriginalQueueMessage.Id)
                {
                    this.innerList[i] = message;
                    return false;
                }
            }

            this.innerList.Add(message);
            return true;
        }

        public void Clear()
        {
            this.innerList.Clear();
        }

        public IEnumerator<MessageData> GetEnumerator()
        {
            return this.innerList.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }
}
