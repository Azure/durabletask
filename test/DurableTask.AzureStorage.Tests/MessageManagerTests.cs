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
    using DurableTask.AzureStorage.Storage;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading.Tasks;

    [TestClass]
    public class MessageManagerTests
    {
        const int MaxStorageQueuePayloadSizeInBytes = 45 * 1024;

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
        [DataRow(false)]
        [DataRow(true)]
        public async Task SerializeMessageDataAsync_InlinePayloadMatchesPreviousWirePayload(bool useDataContractSerialization)
        {
            var binder = new CustomNameTypeBinder();
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                CustomMessageTypeBinder = binder,
                UseDataContractSerialization = useDataContractSerialization,
            };
            var manager = SetupMessageManager(settings, "$root");
            MessageData message = CreateMessageData("inline payload");

            string actualPayload = await manager.SerializeMessageDataAsync(message);
            string previousWirePayload = SerializeUsingMessageSettings(
                new AzureStorageOrchestrationServiceSettings
                {
                    CustomMessageTypeBinder = new CustomNameTypeBinder(),
                    UseDataContractSerialization = useDataContractSerialization,
                },
                message);

            Assert.AreEqual(previousWirePayload, actualPayload);
            Assert.AreEqual(Encoding.UTF8.GetByteCount(previousWirePayload), message.TotalMessageSizeBytes);
            Assert.AreEqual(MessageFormatFlags.InlineJson, manager.GetMessageFormatFlags(message));
            Assert.IsNull(message.CompressedBlobName);
            Assert.AreEqual(1, binder.MessageDataSerializationCount);
            StringAssert.Contains(actualPayload, CustomNameTypeBinder.CustomAssemblyName);

            MessageData roundTrippedMessage = manager.DeserializeMessageData(actualPayload);
            Assert.AreEqual(message.ActivityId, roundTrippedMessage.ActivityId);
            Assert.AreEqual(
                ((GenericEvent)message.TaskMessage.Event).Data,
                ((GenericEvent)roundTrippedMessage.TaskMessage.Event).Data);
        }

        [DataTestMethod]
        [DataRow(false, 0)]
        [DataRow(false, 1)]
        [DataRow(true, 0)]
        [DataRow(true, 1)]
        public async Task SerializeMessageDataAsync_UsesExpectedStoragePathAtThreshold(
            bool useDataContractSerialization,
            int bytesOverThreshold)
        {
            var settings = new AzureStorageOrchestrationServiceSettings
            {
                UseDataContractSerialization = useDataContractSerialization,
            };
            string containerName = $"message-manager-{Guid.NewGuid():N}";
            var manager = SetupMessageManager(settings, containerName);
            int targetSize = MaxStorageQueuePayloadSizeInBytes + bytesOverThreshold;
            MessageData message = CreateMessageDataWithSerializedSize(settings, targetSize);
            string fullMessagePayload = SerializeUsingMessageSettings(settings, message);

            try
            {
                string queuePayload = await manager.SerializeMessageDataAsync(message);
                MessageFormatFlags expectedFormat = bytesOverThreshold == 0
                    ? MessageFormatFlags.InlineJson
                    : MessageFormatFlags.StorageBlob;

                Assert.AreEqual(targetSize, message.TotalMessageSizeBytes);
                Assert.AreEqual(expectedFormat, manager.GetMessageFormatFlags(message));

                if (expectedFormat == MessageFormatFlags.InlineJson)
                {
                    Assert.AreEqual(fullMessagePayload, queuePayload);
                    Assert.IsNull(message.CompressedBlobName);
                }
                else
                {
                    string expectedWrapperPayload = SerializeUsingMessageSettings(
                        settings,
                        new MessageData { CompressedBlobName = message.CompressedBlobName });
                    Assert.AreEqual(expectedWrapperPayload, queuePayload);

                    MessageData wrapper = manager.DeserializeMessageData(queuePayload);
                    Assert.IsNull(wrapper.TaskMessage);
                    Assert.IsFalse(string.IsNullOrWhiteSpace(wrapper.CompressedBlobName));
                    Assert.AreEqual(message.CompressedBlobName, wrapper.CompressedBlobName);

                    string storedPayload = await manager.DownloadAndDecompressAsBytesAsync(wrapper.CompressedBlobName);
                    Assert.AreEqual(fullMessagePayload, storedPayload);
                }
            }
            finally
            {
                if (bytesOverThreshold > 0)
                {
                    await manager.DeleteContainerAsync();
                }
            }
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

        private string GetMessage(string dictionaryType)
            => "{\"$type\":\"DurableTask.AzureStorage.MessageData\",\"ActivityId\":\"5406d369-4369-4673-afae-6671a2fa1e57\",\"TaskMessage\":{\"$type\":\"DurableTask.Core.TaskMessage\",\"Event\":{\"$type\":\"DurableTask.Core.History.ExecutionStartedEvent\",\"OrchestrationInstance\":{\"$type\":\"DurableTask.Core.OrchestrationInstance\",\"InstanceId\":\"2.2-34a2c9d4-306e-4467-8470-a8018b2e4f11\",\"ExecutionId\":\"aae324dcc8f943e490b37ec5e5bbf9da\"},\"EventType\":0,\"ParentInstance\":null,\"Name\":\"OrchestrationName\",\"Version\":\"2.0\",\"Input\":\"input\",\"Tags\":{\"$type\":\""
            + dictionaryType
            + "\",\"tag1\":\"tagValue\"},\"Correlation\":null,\"ScheduledStartTime\":null,\"Generation\":0,\"EventId\":-1,\"IsPlayed\":false,\"Timestamp\":\"2023-03-24T20:53:05.9093518Z\"},\"SequenceNumber\":0,\"OrchestrationInstance\":{\"$type\":\"DurableTask.Core.OrchestrationInstance\",\"InstanceId\":\"2.2-34a2c9d4-306e-4467-8470-a8018b2e4f11\",\"ExecutionId\":\"aae324dcc8f943e490b37ec5e5bbf9da\"}},\"CompressedBlobName\":null,\"SequenceNumber\":40,\"Sender\":{\"InstanceId\":\"\",\"ExecutionId\":\"\"},\"SerializableTraceContext\":null}\r\n\r\n";

        private MessageManager SetupMessageManager(ICustomTypeBinder binder)
        {
            return SetupMessageManager(
                new AzureStorageOrchestrationServiceSettings { CustomMessageTypeBinder = binder },
                "$root");
        }

        private static MessageManager SetupMessageManager(
            AzureStorageOrchestrationServiceSettings settings,
            string containerName)
        {
            settings.StorageAccountClientProvider =
                new StorageAccountClientProvider(TestHelpers.GetTestStorageAccountConnectionString());
            var azureStorageClient = new AzureStorageClient(settings);
            return new MessageManager(settings, azureStorageClient, containerName);
        }

        private static MessageData CreateMessageData(string payload)
        {
            var orchestrationInstance = new OrchestrationInstance
            {
                InstanceId = "message-manager-instance",
                ExecutionId = "message-manager-execution",
            };
            var taskMessage = new TaskMessage
            {
                Event = new GenericEvent(1, payload),
                OrchestrationInstance = orchestrationInstance,
                SequenceNumber = 42,
            };

            return new MessageData(
                taskMessage,
                Guid.Parse("55f31df8-9abb-4d86-a197-78fd0908efcf"),
                "message-manager-queue",
                orchestrationEpisode: 3,
                sender: orchestrationInstance)
            {
                SequenceNumber = 43,
                SerializableTraceContext = "trace-context",
            };
        }

        private static MessageData CreateMessageDataWithSerializedSize(
            AzureStorageOrchestrationServiceSettings settings,
            int targetSize)
        {
            MessageData message = CreateMessageData(string.Empty);
            int emptyPayloadSize = Encoding.UTF8.GetByteCount(SerializeUsingMessageSettings(settings, message));
            Assert.IsTrue(targetSize >= emptyPayloadSize);

            ((GenericEvent)message.TaskMessage.Event).Data = new string('x', targetSize - emptyPayloadSize);
            int actualSize = Encoding.UTF8.GetByteCount(SerializeUsingMessageSettings(settings, message));
            Assert.AreEqual(targetSize, actualSize);
            return message;
        }

        private static string SerializeUsingMessageSettings(
            AzureStorageOrchestrationServiceSettings settings,
            MessageData message)
        {
            var serializerSettings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Objects,
                SerializationBinder = new TypeNameSerializationBinder(settings.CustomMessageTypeBinder),
            };

            if (settings.UseDataContractSerialization)
            {
                serializerSettings.Converters.Add(new DataContractJsonConverter());
            }

            return Utils.SerializeToJson(JsonSerializer.Create(serializerSettings), message);
        }
    }

    internal class CustomNameTypeBinder : ICustomTypeBinder
    {
        public const string CustomAssemblyName = "MessageManagerTests.CustomAssembly";
        readonly Dictionary<string, Type> serializedTypes = new Dictionary<string, Type>();

        public int MessageDataSerializationCount { get; private set; }

        public void BindToName(Type serializedType, out string assemblyName, out string typeName)
        {
            assemblyName = CustomAssemblyName;
            typeName = serializedType.FullName!;
            this.serializedTypes[typeName] = serializedType;

            if (serializedType == typeof(MessageData))
            {
                this.MessageDataSerializationCount++;
            }
        }

        public Type BindToType(string assemblyName, string typeName)
        {
            if (this.serializedTypes.TryGetValue(typeName, out Type? serializedType))
            {
                return serializedType;
            }

            throw new JsonSerializationException($"Unknown serialized type '{typeName}'.");
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
