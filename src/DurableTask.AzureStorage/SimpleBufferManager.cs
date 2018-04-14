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
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using Microsoft.WindowsAzure.Storage;

    /// <summary>
    /// Simple buffer manager intended for use with Azure Storage SDK and compression code.
    /// It is not intended to be robust enough for external use.
    /// </summary>
    class SimpleBufferManager : IBufferManager
    {
        internal const int MaxBufferSize = 1024 * 1024; //  1 MB
        const int DefaultBufferSize = 64 * 1024;        // 64 KB

        /// <summary>
        /// Shared singleton instance of <see cref="SimpleBufferManager"/>.
        /// </summary>
        public static SimpleBufferManager Shared { get; } = new SimpleBufferManager();

        /// <summary>
        /// Internal pool of buffers. Using stacks internally ensures that the same pool can be
        /// frequently reused, which can result in improved performance due to hardware caching.
        /// </summary>
        readonly ConcurrentDictionary<int, ConcurrentStack<byte[]>> pool;

        int allocatedBytes;
        int availableBytes;

        /// <summary>
        /// Initializes a new instance of the <see cref="SimpleBufferManager"/> class.
        /// </summary>
        public SimpleBufferManager()
        {
            this.pool = new ConcurrentDictionary<int, ConcurrentStack<byte[]>>();
        }

        /// <summary>
        /// The total number of bytes allocated by this buffer.
        /// </summary>
        public int AllocatedBytes => this.allocatedBytes;

        /// <summary>
        /// The total bytes available to be reused.
        /// </summary>
        public int AvailableBytes => this.availableBytes;

        /// <summary>
        /// The number of buckets allocated by this pool.
        /// </summary>
        public int BucketCount => this.pool.Count;

        /// <inheritdoc />
        public int GetDefaultBufferSize()
        {
            return DefaultBufferSize;
        }

        /// <inheritdoc />
        public void ReturnBuffer(byte[] buffer)
        {
            if (buffer == null)
            {
                throw new ArgumentNullException(nameof(buffer));
            }

            int bufferSize = buffer.Length;
            if (bufferSize > MaxBufferSize)
            {
                // This was a large buffer which we're not tracking.
                return;
            }

            ConcurrentStack<byte[]> bucket;
            if (!this.pool.TryGetValue(bufferSize, out bucket))
            {
                throw new ArgumentException("The returned buffer did not come from this pool.", nameof(buffer));
            }

            bucket.Push(buffer);
            Interlocked.Add(ref this.availableBytes, bufferSize);
        }

        /// <inheritdoc />
        public byte[] TakeBuffer(int bufferSize)
        {
            if (bufferSize > MaxBufferSize)
            {
                // We don't track large buffers
                return new byte[bufferSize];
            }

            bufferSize = (bufferSize < 0) ? DefaultBufferSize : bufferSize;

            ConcurrentStack<byte[]> bucket = this.pool.GetOrAdd(
                bufferSize,
                size => new ConcurrentStack<byte[]>());

            byte[] buffer;
            if (bucket.TryPop(out buffer))
            {
                Interlocked.Add(ref this.availableBytes, -bufferSize);
            }
            else
            {
                buffer = new byte[bufferSize];
                Interlocked.Add(ref this.allocatedBytes, bufferSize);
            }

            return buffer;
        }

        /// <summary>
        /// Returns a debug string representing the current state of the buffer manager.
        /// </summary>
        public override string ToString()
        {
            return string.Format(
                "BucketCount: {0}, AvailableBytes: {1}, AllocatedBytes: {2}.",
                this.BucketCount,
                this.AvailableBytes,
                this.AllocatedBytes);
        }
    }
}
