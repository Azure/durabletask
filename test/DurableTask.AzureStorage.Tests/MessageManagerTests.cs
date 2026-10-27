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
#nullable enable
namespace DurableTask.AzureStorage.Tests
{
    using Azure.Storage.Blobs;
    using Azure.Storage.Blobs.Specialized;
    using DurableTask.AzureStorage.Storage;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;
    using Moq.Protected;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    [TestClass]
    public class MessageManagerTests
    {
        [DataTestMethod]
        [DataRow("System.Collections.Generic.Dictionary`2[[System.String, System.Private.CoreLib],[System.String, System.Private.CoreLib]]")]
        [DataRow("System.Collections.Generic.Dictionary`2[[System.String, mscorlib],[System.String, mscorlib]]")]
        public void DeserializesStandardTypes(string dictionaryType)
        {
            // Given
            var message = GetMessage(dictionaryType);
            var messageManager = SetupMessageManager(new PrimitiveTypeBinder());

            // When
            var deserializedMessage = messageManager.DeserializeMessageData(message);

            // Then
            Assert.IsInstanceOfType(deserializedMessage.TaskMessage.Event, typeof(ExecutionStartedEvent));
            ExecutionStartedEvent startedEvent = (ExecutionStartedEvent)deserializedMessage.TaskMessage.Event;
            Assert.AreEqual("tagValue", startedEvent.Tags["tag1"]);
        }

        [TestMethod]
        public void FailsDeserializingUnknownTypes()
        {
            // Given
            var message = GetMessage("RandomType");
            var messageManager = SetupMessageManager(new KnownTypeBinder());

            // When/Then
            Assert.ThrowsException<JsonSerializationException>(() => messageManager.DeserializeMessageData(message));
        }


        [TestMethod]
        public void DeserializesCustomTypes()
        {
            // Given
            var message = GetMessage("KnownType");
            var messageManager = SetupMessageManager(new KnownTypeBinder());

            // When
            var deserializedMessage = messageManager.DeserializeMessageData(message);

            // Then
            Assert.IsInstanceOfType(deserializedMessage.TaskMessage.Event, typeof(ExecutionStartedEvent));
            ExecutionStartedEvent startedEvent = (ExecutionStartedEvent)deserializedMessage.TaskMessage.Event;
            Assert.AreEqual("tagValue", startedEvent.Tags["tag1"]);
        }

        [DataTestMethod]
        [DataRow("blob.bin", "blob.bin")]
        [DataRow("@#$%!", "%40%23%24%25%21")]
        [DataRow("foo/bar/b@z.tar.gz", "foo/bar/b%40z.tar.gz")]
        public void GetBlobUrlEscaped(string blob, string blobUrl)
        {
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider("UseDevelopmentStorage=true"),
            };

            const string container = "@entity12345";
            var manager = new MessageManager(settings, new AzureStorageClient(settings), container);

            var expected = $"http://127.0.0.1:10000/devstoreaccount1/{container}/{blobUrl}";
            Assert.AreEqual(expected, manager.GetBlobUrl(blob));
        }

        [DataTestMethod]
        [DataRow(true, "instance/message-0123456789abcdef0123456789abcdef-ExecutionStarted.json.gz")]
        [DataRow(false, "instance/message-0123456789abcdef0123456789abcdef-ExecutionStarted.json.gz")]
        [DataRow(true, "instance/history-0000000000000001-ExecutionCompleted-12345678-Result.json.gz")]
        [DataRow(false, "instance/history-0000000000000001-ExecutionCompleted-12345678-Result.json.gz")]
        [DataRow(true, "instance|with spaces/history-0000000000000001-ExecutionStarted-12345678-Input.json.gz")]
        [DataRow(false, "instance|with spaces/history-0000000000000001-ExecutionStarted-12345678-Input.json.gz")]
        [DataRow(true, "instance/encoded%2Fname.json.gz")]
        [DataRow(false, "instance/encoded%2Fname.json.gz")]
        public void TryGetLargeMessageReference_AcceptsTaskHubReferences(bool useEmulator, string blobName)
        {
            var manager = SetupLargeMessageManager(useEmulator);
            string reference = manager.GetBlobUrl(blobName);

            Assert.IsTrue(manager.TryGetLargeMessageReference(reference, out Uri blobUri));
            Assert.AreEqual(reference, blobUri.AbsoluteUri);
        }

        [DataTestMethod]
        [DataRow(null)]
        [DataRow("")]
        [DataRow("ordinary termination reason")]
        [DataRow("https://example.com/backup.json.gz")]
        [DataRow("http://127.0.0.1:10000/devstoreaccount1/ownerprivate/backup.json.gz")]
        [DataRow("http://127.0.0.1:10000/devstoreaccount1/other-largemessages/backup.json.gz")]
        [DataRow("http://127.0.0.1:10000/devstoreaccount1/test-largemessages-other/backup.json.gz")]
        [DataRow("http://127.0.0.1:10000/otheraccount/test-largemessages/backup.json.gz")]
        [DataRow("http://127.0.0.1:10001/devstoreaccount1/test-largemessages/backup.json.gz")]
        [DataRow("https://127.0.0.1:10000/devstoreaccount1/test-largemessages/backup.json.gz")]
        [DataRow("http://127.0.0.1:10000/devstoreaccount1/Test-largemessages/backup.json.gz")]
        [DataRow("http://127.0.0.1:10000/devstoreaccount1/test-largemessages")]
        [DataRow("http://127.0.0.1:10000/devstoreaccount1/test-largemessages/")]
        [DataRow("http://127.0.0.1:10000/devstoreaccount1/test-largemessages/blob.json.gz?comp=metadata")]
        [DataRow("http://127.0.0.1:10000/devstoreaccount1/test-largemessages/blob.json.gz#fragment")]
        [DataRow("http://user@127.0.0.1:10000/devstoreaccount1/test-largemessages/blob.json.gz")]
        [DataRow("http://127.0.0.1:10000/devstoreaccount1/test-largemessages/../ownerprivate/backup.json.gz")]
        [DataRow("http://127.0.0.1:10000/devstoreaccount1/test-largemessages/%2e%2e/ownerprivate/backup.json.gz")]
        public async Task FetchLargeMessageIfNecessary_OutOfScopePayload_RemainsLiteral(string? payload)
        {
            var manager = SetupLargeMessageManager(useEmulator: true);

            Assert.IsFalse(manager.TryGetLargeMessageReference(payload, out Uri blobUri));
            Assert.IsNull(blobUri);
            Assert.AreEqual(payload, await manager.FetchLargeMessageIfNecessary(payload));

            if (Uri.TryCreate(payload, UriKind.Absolute, out Uri? candidate) && candidate != null)
            {
                await Assert.ThrowsExceptionAsync<ArgumentException>(() => manager.DownloadAndDecompressAsBytesAsync(candidate));
            }
        }

        [DataTestMethod]
        [DataRow("https://referencevalidation.blob.core.windows.net/other-largemessages/message.json.gz")]
        [DataRow("https://referencevalidation.blob.core.windows.net/test-largemessages-other/message.json.gz")]
        [DataRow("https://otheraccount.blob.core.windows.net/test-largemessages/message.json.gz")]
        [DataRow("https://referencevalidation.blob.core.windows.net/test-largemessages/")]
        [DataRow("https://referencevalidation.blob.core.windows.net/test-largemessages/message.json.gz?comp=metadata")]
        public async Task FetchLargeMessageIfNecessary_ReconstructedTargetOutsideScope_RemainsLiteral(string targetUri)
        {
            const string reference = "https://referencevalidation.blob.core.windows.net/test-largemessages/message.json.gz";
            var blob = new Mock<BlockBlobClient>(MockBehavior.Strict);
            blob.SetupGet(client => client.Uri).Returns(new Uri(targetUri));

            var container = new Mock<BlobContainerClient>(MockBehavior.Strict);
            container.SetupGet(client => client.Uri)
                .Returns(new Uri("https://referencevalidation.blob.core.windows.net/test-largemessages"));
            container.Protected()
                .Setup<BlockBlobClient>("GetBlockBlobClientCore", ItExpr.IsAny<string>())
                .Returns(blob.Object);

            var service = new Mock<BlobServiceClient>(MockBehavior.Strict);
            service.Setup(client => client.GetBlobContainerClient("test-largemessages")).Returns(container.Object);

            var blobProvider = new Mock<IStorageServiceClientProvider<BlobServiceClient, BlobClientOptions>>(MockBehavior.Strict);
            blobProvider.Setup(provider => provider.CreateOptions()).Returns(new BlobClientOptions());
            blobProvider.Setup(provider => provider.CreateClient(It.IsAny<BlobClientOptions>())).Returns(service.Object);

            var provider = new StorageAccountClientProvider("UseDevelopmentStorage=true");
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider(blobProvider.Object, provider.Queue, provider.Table),
            };
            var manager = new MessageManager(settings, new AzureStorageClient(settings), "test-largemessages");

            Assert.IsFalse(manager.TryGetLargeMessageReference(reference, out Uri blobUri));
            Assert.IsNull(blobUri);
            Assert.AreEqual(reference, await manager.FetchLargeMessageIfNecessary(reference));
            await Assert.ThrowsExceptionAsync<ArgumentException>(() => manager.DownloadAndDecompressAsBytesAsync(new Uri(reference)));
        }

        private static MessageManager SetupLargeMessageManager(bool useEmulator)
        {
            string connectionString = useEmulator
                ? "UseDevelopmentStorage=true"
                : $"DefaultEndpointsProtocol=https;AccountName=referencevalidation;AccountKey={Convert.ToBase64String(new byte[32])};EndpointSuffix=core.windows.net";
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageAccountClientProvider = new StorageAccountClientProvider(connectionString),
            };

            return new MessageManager(settings, new AzureStorageClient(settings), "test-largemessages");
        }

        private string GetMessage(string dictionaryType)
            => "{\"$type\":\"DurableTask.AzureStorage.MessageData\",\"ActivityId\":\"5406d369-4369-4673-afae-6671a2fa1e57\",\"TaskMessage\":{\"$type\":\"DurableTask.Core.TaskMessage\",\"Event\":{\"$type\":\"DurableTask.Core.History.ExecutionStartedEvent\",\"OrchestrationInstance\":{\"$type\":\"DurableTask.Core.OrchestrationInstance\",\"InstanceId\":\"2.2-34a2c9d4-306e-4467-8470-a8018b2e4f11\",\"ExecutionId\":\"aae324dcc8f943e490b37ec5e5bbf9da\"},\"EventType\":0,\"ParentInstance\":null,\"Name\":\"OrchestrationName\",\"Version\":\"2.0\",\"Input\":\"input\",\"Tags\":{\"$type\":\""
            + dictionaryType
            + "\",\"tag1\":\"tagValue\"},\"Correlation\":null,\"ScheduledStartTime\":null,\"Generation\":0,\"EventId\":-1,\"IsPlayed\":false,\"Timestamp\":\"2023-03-24T20:53:05.9093518Z\"},\"SequenceNumber\":0,\"OrchestrationInstance\":{\"$type\":\"DurableTask.Core.OrchestrationInstance\",\"InstanceId\":\"2.2-34a2c9d4-306e-4467-8470-a8018b2e4f11\",\"ExecutionId\":\"aae324dcc8f943e490b37ec5e5bbf9da\"}},\"CompressedBlobName\":null,\"SequenceNumber\":40,\"Sender\":{\"InstanceId\":\"\",\"ExecutionId\":\"\"},\"SerializableTraceContext\":null}\r\n\r\n";

        private MessageManager SetupMessageManager(ICustomTypeBinder binder)
        {
            var azureStorageClient = new AzureStorageClient(
                new AzureStorageOrchestrationServiceSettings
                {
                    StorageAccountClientProvider = new StorageAccountClientProvider("UseDevelopmentStorage=true"),
                });

            return new MessageManager(
                new AzureStorageOrchestrationServiceSettings { CustomMessageTypeBinder = binder },
                azureStorageClient,
                "$root");
        }
    }

    internal class KnownTypeBinder : ICustomTypeBinder
    {
        public void BindToName(Type serializedType, out string assemblyName, out string typeName)
        {
            throw new NotImplementedException();
        }

        public Type? BindToType(string assemblyName, string typeName)
        {
            if (typeName == "KnownType")
            {
                return typeof(Dictionary<string, string>);
            }

            return null;
        }
    }

    internal class PrimitiveTypeBinder : ICustomTypeBinder
    {
        readonly bool hasStandardLib;

        public PrimitiveTypeBinder() 
        {
            hasStandardLib = typeof(string).AssemblyQualifiedName!.Contains("mscorlib");
        }

        public void BindToName(Type serializedType, out string assemblyName, out string typeName)
        {
            throw new NotImplementedException();
        }

        public Type BindToType(string assemblyName, string typeName)
        {
            if (hasStandardLib)
            {
                return Type.GetType(typeName.Replace("System.Private.CoreLib", "mscorlib"))!;
            }

            return Type.GetType(typeName.Replace("mscorlib", "System.Private.CoreLib"))!;
        }
    }
}
