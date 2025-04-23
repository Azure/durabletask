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
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;

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
