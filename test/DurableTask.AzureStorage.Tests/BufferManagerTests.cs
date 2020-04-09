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

namespace DurableTask.AzureStorage.Tests
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class BufferManagerTests
    {
        /// <summary>
        /// Validates basic take and return operations.
        /// </summary>
        [TestMethod]
        public void TakeAndReturnBuffer()
        {
            var bufferManager = new SimpleBufferManager();
            Trace.TraceInformation(bufferManager.ToString());
            Assert.AreEqual(0, bufferManager.BucketCount);
            Assert.AreEqual(0, bufferManager.AllocatedBytes);
            Assert.AreEqual(0, bufferManager.AvailableBytes);

            byte[] buffer1024 = bufferManager.TakeBuffer(1024);
            Trace.TraceInformation(bufferManager.ToString());
            Assert.AreEqual(1024, buffer1024.Length);
            Assert.AreEqual(1024, bufferManager.AllocatedBytes);
            Assert.AreEqual(1, bufferManager.BucketCount);
            Assert.AreEqual(0, bufferManager.AvailableBytes);

            bufferManager.ReturnBuffer(buffer1024);
            Trace.TraceInformation(bufferManager.ToString());
            Assert.AreEqual(1024, bufferManager.AllocatedBytes);
            Assert.AreEqual(1024, bufferManager.AvailableBytes);
            Assert.AreEqual(1, bufferManager.BucketCount);

            byte[] buffer4096 = bufferManager.TakeBuffer(4096);
            Trace.TraceInformation(bufferManager.ToString());
            Assert.AreEqual(4096, buffer4096.Length);
            Assert.AreEqual(1024 + 4096, bufferManager.AllocatedBytes);
            Assert.AreEqual(1024, bufferManager.AvailableBytes);
            Assert.AreEqual(2, bufferManager.BucketCount);

            bufferManager.ReturnBuffer(buffer4096);
            Trace.TraceInformation(bufferManager.ToString());
            Assert.AreEqual(1024 + 4096, bufferManager.AllocatedBytes);
            Assert.AreEqual(1024 + 4096, bufferManager.AvailableBytes);
            Assert.AreEqual(2, bufferManager.BucketCount);
        }

        /// <summary>
        /// Tests that buffers of the same size are correctly reused.
        /// </summary>
        [TestMethod]
        public void RecycleBuffer()
        {
            var bufferManager = new SimpleBufferManager();
            int bufferSize = bufferManager.GetDefaultBufferSize();

            for (int i = 0; i < 10; i++)
            {
                byte[] buffer = bufferManager.TakeBuffer(bufferSize);
                Trace.TraceInformation(bufferManager.ToString());
                Assert.AreEqual(bufferSize, buffer.Length);
                Assert.AreEqual(bufferSize, bufferManager.AllocatedBytes);
                Assert.AreEqual(0, bufferManager.AvailableBytes);
                Assert.AreEqual(1, bufferManager.BucketCount);

                bufferManager.ReturnBuffer(buffer);
                Trace.TraceInformation(bufferManager.ToString());
                Assert.AreEqual(bufferSize, bufferManager.AllocatedBytes);
                Assert.AreEqual(bufferSize, bufferManager.AvailableBytes);
                Assert.AreEqual(1, bufferManager.BucketCount);
            }
        }

        /// <summary>
        /// Tests concurrent buffer allocation and deallocation.
        /// </summary>
        [TestMethod]
        public void ConcurrentAllocations()
        {
            var bufferManager = new SimpleBufferManager();
            int bufferSize = bufferManager.GetDefaultBufferSize();

            const int ConcurrentThreads = 16;
            var options = new ParallelOptions
            {
                MaxDegreeOfParallelism = ConcurrentThreads,
            };

            // Repeat the core test multiple times to ensure the exact same
            // results each time (verifies the recycling behavior).
            for (int i = 0; i < 5; i++)
            {
                var uniqueBuffers = new HashSet<byte[]>();
                Parallel.For(0, ConcurrentThreads, options, j =>
                {
                    byte[] buffer = bufferManager.TakeBuffer(bufferSize);
                    Assert.AreEqual(bufferSize, buffer.Length);
                    lock (uniqueBuffers)
                    {
                        Assert.AreEqual(true, uniqueBuffers.Add(buffer));
                    }
                });

                Trace.TraceInformation($"Round {i} after allocation: {bufferManager}");
                Assert.AreEqual(ConcurrentThreads * bufferSize, bufferManager.AllocatedBytes);
                Assert.AreEqual(ConcurrentThreads, uniqueBuffers.Count);
                Assert.AreEqual(0, bufferManager.AvailableBytes);
                Assert.AreEqual(1, bufferManager.BucketCount);

                Parallel.ForEach(uniqueBuffers, options, buffer =>
                {
                    bufferManager.ReturnBuffer(buffer);
                });

                Trace.TraceInformation($"Round {i} after deallocation: {bufferManager}");
                Assert.AreEqual(ConcurrentThreads * bufferSize, bufferManager.AvailableBytes);
                Assert.AreEqual(bufferManager.AvailableBytes, bufferManager.AllocatedBytes);
                Assert.AreEqual(1, bufferManager.BucketCount);
            }
        }

        /// <summary>
        /// Verifies that the buffer manager can manage multiple buckets of different sizes.
        /// </summary>
        [TestMethod]
        public void MultipleBuckets()
        {
            var bufferManager = new SimpleBufferManager();

            var buffers = new HashSet<byte[]>();

            int totalBytes = 0;
            int bucketCount;
            for (bucketCount = 1; bucketCount <= 3; bucketCount++)
            {
                int bufferSize = 1024 << (bucketCount - 1);
                for (int i = 1; i <= 4; i++)
                {
                    byte[] buffer = bufferManager.TakeBuffer(bufferSize);
                    Assert.AreEqual(bufferSize, buffer.Length);
                    Assert.IsTrue(buffers.Add(buffer));

                    totalBytes += bufferSize;

                    Trace.TraceInformation($"Bucket {bucketCount}, round {i}: {bufferManager}");

                    Assert.AreEqual(totalBytes, bufferManager.AllocatedBytes);
                    Assert.AreEqual(bucketCount, bufferManager.BucketCount);
                    Assert.AreEqual(0, bufferManager.AvailableBytes);
                }
            }

            bucketCount--;

            int returnedBytes = 0;
            foreach (byte[] buffer in buffers)
            {
                bufferManager.ReturnBuffer(buffer);

                returnedBytes += buffer.Length;

                Trace.TraceInformation($"After returning {returnedBytes} bytes: {bufferManager}");
                Assert.AreEqual(bucketCount, bufferManager.BucketCount);
                Assert.AreEqual(returnedBytes, bufferManager.AvailableBytes);
            }
        }

        /// <summary>
        /// Verifies that large buffers do not get allocated to the pool.
        /// </summary>
        [TestMethod]
        public void LargeBuffer()
        {
            var bufferManager = new SimpleBufferManager();

            int bufferSize = SimpleBufferManager.MaxBufferSize + 1;
            byte[] buffer = bufferManager.TakeBuffer(bufferSize);
            Trace.TraceInformation(bufferManager.ToString());
            Assert.AreEqual(bufferSize, buffer.Length);
            Assert.AreEqual(0, bufferManager.AllocatedBytes);
            Assert.AreEqual(0, bufferManager.AvailableBytes);
            Assert.AreEqual(0, bufferManager.BucketCount);

            bufferManager.ReturnBuffer(buffer);
            Trace.TraceInformation(bufferManager.ToString());
            Assert.AreEqual(0, bufferManager.AllocatedBytes);
            Assert.AreEqual(0, bufferManager.AvailableBytes);
            Assert.AreEqual(0, bufferManager.BucketCount);
        }
    }
}
