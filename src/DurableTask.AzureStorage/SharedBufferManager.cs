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
    using System.ServiceModel.Channels;
    using System.Timers;

    using Microsoft.WindowsAzure.Storage;

    /// <summary>
    /// Shared BufferManager to limit the temporary memory footprint of compression/decompression of large messages.
    /// </summary>
    public sealed class SharedBufferManager : IBufferManager
    {
        static SharedBufferManager instance = null;
        static readonly object padlock = new object();

        static Timer timer;
        static readonly object sharedlock = new object();
        static BufferManager innerBufferManager;
        static bool isInitialized;
        static readonly long defaultMaxBufferPoolSize = 0; // Avoid pooling for large messages, reference: http://andriybuday.com/2011/08/wcf-configuration-caused-memory-leaks.html
        static readonly int defaultBufferSize = 256 * 1024; // 256 KB
        static readonly int defaultClearBufferIntervalInMinutes = 10;

        /// <summary>
        /// The BufferManager
        /// </summary>
        SharedBufferManager()
        { }

        /// <summary>
        /// The BufferManager instance
        /// </summary>
        public static SharedBufferManager Instance
        {
            get
            {
                if (instance == null)
                {
                    lock (padlock)
                    {
                        if (instance == null)
                        {
                            instance = new SharedBufferManager();
                        }

                        if (!isInitialized)
                        {
                            Initialize(defaultMaxBufferPoolSize, defaultBufferSize);
                        }
                    }
                }

                return instance;
            }
        }

        /// <summary>
        /// Checks if buffer is initialized
        /// </summary>
        public bool IsInitialized()
        {
            return isInitialized;
        }

        /// <summary>
        /// Gets the default buffer size.
        /// </summary>
        public int GetDefaultBufferSize()
        {
            return defaultBufferSize;
        }

        /// <summary>
        /// Returns a buffer to the buffer pool.
        /// </summary>
        public void ReturnBuffer(byte[] buffer)
        {
            // Return buffer to pool, and set the memory to null.
            // Reference: https://www.wintellect.com/pooling-buffers-for-better-memory-management/
            lock (sharedlock)
            {
                innerBufferManager.ReturnBuffer(buffer);
                buffer = null;
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
                bufferSize = (bufferSize < 0) ? defaultBufferSize : bufferSize;
                return innerBufferManager.TakeBuffer(bufferSize);
            }
        }

        /// <summary>
        /// Initialize the Buffer Manager
        /// </summary>
        internal static void Initialize(long maxBufferPoolSize, int maxBufferSize)
        {
            ValidateParams(maxBufferPoolSize, maxBufferSize);
            lock (sharedlock)
            {
                innerBufferManager = BufferManager.CreateBufferManager(maxBufferPoolSize, maxBufferSize);
                isInitialized = true;
            }
            ScheduleTimer();
        }

        /// <summary>
        /// Schedule timer to periodically clear the buffer pool.
        /// </summary>
        internal static void ScheduleTimer()
        {
            DateTime nowTime = DateTime.UtcNow;
            DateTime scheduledTime = DateTime.UtcNow;
            if (nowTime >= scheduledTime)
            {
                scheduledTime = scheduledTime.AddMinutes(defaultClearBufferIntervalInMinutes);
            }

            double tickTime = (double)(scheduledTime - DateTime.Now).TotalMilliseconds;
            timer = new Timer(tickTime);
            timer.Elapsed += new ElapsedEventHandler(TimerElapsed);
            timer.Start();
        }

        /// <summary>
        /// Initiate clearing buffer pool when timer fires
        /// </summary>
        internal static void TimerElapsed(object sender, ElapsedEventArgs e)
        {
            timer.Stop();
            Clear();
            ScheduleTimer();
        }

        /// <summary>
        /// Validates the maximum buffer pool and maximum buffer size.
        /// </summary>
        internal static void ValidateParams(long maxBufferPoolSize, int maxBufferSize)
        {
            if (maxBufferPoolSize < 0)
            {
                throw new ArgumentException($"Invalid maxBufferPoolSize '{maxBufferPoolSize}'. maxBufferPoolSize should be >= 0.");
            }

            if (maxBufferSize < 0)
            {
                throw new ArgumentException($"Invalid maxBufferSize '{maxBufferSize}'. maxBufferSize should be >= 0.");
            }
        }
    }
}
