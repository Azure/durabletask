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

namespace DurableTask.ServiceBus.Tests
{
    using System;
    using System.IO;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.Common;
    using DurableTask.Core.Settings;
    using DurableTask.Core.Tracking;
    using DurableTask.ServiceBus.Common.Abstraction;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class ServiceBusUtilsTests
    {
        const string Payload = "inline message payload";

        [TestMethod]
        public Task UncompressedInlineMessageDeserializationCompletesSynchronously()
        {
            return AssertInlineMessageRoundTrips(CompressionStyle.Never, expectSynchronousCompletion: true);
        }

        [TestMethod]
        public Task CompressedInlineMessageDeserializationRoundTrips()
        {
            return AssertInlineMessageRoundTrips(CompressionStyle.Always, expectSynchronousCompletion: false);
        }

        [TestMethod]
        public async Task ExternalMessageDeserializationAwaitsBlobStore()
        {
            const string blobKey = "message-blob";
            var message = new Message();
            message.UserProperties[ServiceBusConstants.MessageBlobKey] = blobKey;
            message.UserProperties[FrameworkConstants.CompressionTypePropertyName] =
                FrameworkConstants.CompressionTypeNonePropertyValue;

            var stream = new MemoryStream();
            Utils.WriteObjectToStream(stream, Payload);
            stream.Position = stream.Length;

            var blobStore = new DeferredBlobStore();
            Task<string> deserializationTask =
                ServiceBusUtils.GetObjectFromBrokeredMessageAsync<string>(message, blobStore);

            Assert.AreEqual(blobKey, blobStore.LoadedBlobKey);
            Assert.IsFalse(deserializationTask.IsCompleted);

            blobStore.CompleteLoad(stream);

            Assert.AreEqual(Payload, await deserializationTask);
            Assert.ThrowsException<ObjectDisposedException>(() => stream.ReadByte());
        }

        static async Task AssertInlineMessageRoundTrips(
            CompressionStyle compressionStyle,
            bool expectSynchronousCompletion)
        {
            Message message = await ServiceBusUtils.GetBrokeredMessageFromObjectAsync(
                Payload,
                new CompressionSettings { Style = compressionStyle });

            Task<string> deserializationTask =
                ServiceBusUtils.GetObjectFromBrokeredMessageAsync<string>(message, null);

            if (expectSynchronousCompletion)
            {
                Assert.IsTrue(
                    deserializationTask.IsCompleted,
                    "Inline message deserialization should not require a thread-pool continuation.");
            }

            Assert.AreEqual(Payload, await deserializationTask);
        }

        sealed class DeferredBlobStore : IOrchestrationServiceBlobStore
        {
            readonly TaskCompletionSource<Stream> loadCompletionSource = new TaskCompletionSource<Stream>();

            public string LoadedBlobKey { get; private set; }

            public string BuildMessageBlobKey(OrchestrationInstance orchestrationInstance, DateTime messageFireTime)
            {
                throw new NotSupportedException();
            }

            public string BuildSessionBlobKey(string sessionId)
            {
                throw new NotSupportedException();
            }

            public Task SaveStreamAsync(string blobKey, Stream stream)
            {
                throw new NotSupportedException();
            }

            public Task<Stream> LoadStreamAsync(string blobKey)
            {
                this.LoadedBlobKey = blobKey;
                return this.loadCompletionSource.Task;
            }

            public Task DeleteStoreAsync()
            {
                throw new NotSupportedException();
            }

            public Task PurgeExpiredBlobsAsync(DateTime thresholdDateTimeUtc)
            {
                throw new NotSupportedException();
            }

            public void CompleteLoad(Stream stream)
            {
                this.loadCompletionSource.SetResult(stream);
            }
        }
    }
}
