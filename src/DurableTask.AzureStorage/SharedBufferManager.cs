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

namespace DurableTask.AzureStorage
{
    using System.ServiceModel.Channels;
    using Microsoft.WindowsAzure.Storage;

    /// <summary>
    /// Shared BufferManager to limit the temporary memory footprint of compression/decompression of large messages.
    /// </summary>
    public class SharedBufferManager : IBufferManager
    {
        static readonly object sharedlock = new object();
        static BufferManager innerBufferManager;
        const long DefaultMaxBufferPoolSize = 0; // Avoid pooling for large messages, reference: http://andriybuday.com/2011/08/wcf-configuration-caused-memory-leaks.html
        const int DefaultBufferSize = 256 * 1024; // 256 KB

        /// <summary>
        /// The BufferManager
        /// </summary>
        public SharedBufferManager()
        {
            innerBufferManager = BufferManager.CreateBufferManager(DefaultMaxBufferPoolSize, DefaultBufferSize);
        }

        /// <summary>
        /// Gets the default buffer size.
        /// </summary>
        public int GetDefaultBufferSize()
        {
            return DefaultBufferSize;
        }

        /// <summary>
        /// Returns a buffer to the buffer pool.
        /// </summary>
        public void ReturnBuffer(byte[] buffer)
        {
            lock (sharedlock)
            {
                innerBufferManager.ReturnBuffer(buffer);
            }
        }

        /// <summary>
        /// Clears the buffer pool.
        /// </summary>
        public static void Clear()
        {
            lock (sharedlock)
            {
                innerBufferManager.Clear();
            }
        }

        /// <summary>
        /// Takes a buffer from the buffer pool.
        /// </summary>
        public byte[] TakeBuffer(int bufferSize)
        {
            lock (sharedlock)
            {
                bufferSize = (bufferSize < 0) ? DefaultBufferSize : bufferSize;
                return innerBufferManager.TakeBuffer(bufferSize);
            }
        }
    }
}
