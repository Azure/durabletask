using DurableTask.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using System;
using System.Runtime.Serialization;

namespace DurableTask.AzureStorage.Tests
{
    [TestClass]
    public class DataContractJsonConverterTests
    {
        private static readonly JsonSerializerSettings serializerSettings = new JsonSerializerSettings
        {
            Converters =
            {
                new DataContractJsonConverter(),
            }
        };

        [TestMethod]
        public void ReadWriteJson_Succeeds()
        {
            var instance = new TestOrchestrationInstance
            {
                InstanceId = Guid.NewGuid().ToString(),
                ExecutionId = Guid.NewGuid().ToString(),
                Extra = Guid.NewGuid().ToString(),
            };

            // Writing a more derived type, and then reading it will populate ExtensionData.
            string json = Serialize(instance);
            OrchestrationInstance actual = Deserialize<OrchestrationInstance>(json);

            Assert.AreEqual(instance.ExecutionId, actual.ExecutionId);
            Assert.AreEqual(instance.InstanceId, actual.InstanceId);
            Assert.IsNotNull(actual.ExtensionData);

            // Writing an instance with ExtensionData, then reading it as a more derived type will repopulate extra fields.
            json = Serialize(actual);
            TestOrchestrationInstance actual2 = Deserialize<TestOrchestrationInstance>(json);

            Assert.AreEqual(instance.ExecutionId, actual2.ExecutionId);
            Assert.AreEqual(instance.InstanceId, actual2.InstanceId);
            Assert.AreEqual(instance.Extra, actual2.Extra);
        }

        [TestMethod]
        public void ReadWriteNull_Succeeds()
        {
            var container = new Container();
            string json = Serialize(container);
            var actual = Deserialize<Container>(json);
            Assert.IsNull(container.Instance);
        }

        private static string Serialize(object value)
            => JsonConvert.SerializeObject(value, serializerSettings);

        private static T Deserialize<T>(string json)
            => JsonConvert.DeserializeObject<T>(json, serializerSettings);

        [DataContract(
            Name = "OrchestrationInstance",
            Namespace = "http://schemas.datacontract.org/2004/07/DurableTask.Core")]
        private class TestOrchestrationInstance : OrchestrationInstance
        {
            [DataMember]
            public string Extra { get; set; }
        }

        private class Container
        {
            public OrchestrationInstance Instance { get; set; }
        }
    }
}
