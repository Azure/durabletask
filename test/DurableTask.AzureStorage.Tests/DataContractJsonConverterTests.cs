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
        [TestMethod]
        public void ReadWriteJson_Succeeds()
        {
            var instance = new TestOrchestrationInstance
            {
                InstanceId = Guid.NewGuid().ToString(),
                ExecutionId = Guid.NewGuid().ToString(),
                Extra = Guid.NewGuid().ToString(),
            };

            var settings = new JsonSerializerSettings
            {
                Converters =
                {
                    new DataContractJsonConverter(),
                }
            };

            // Writing a more derived type, and then reading it will populate ExtensionData.
            string json = JsonConvert.SerializeObject(instance, settings);
            OrchestrationInstance actual = JsonConvert.DeserializeObject<OrchestrationInstance>(json, settings);

            Assert.AreEqual(instance.ExecutionId, actual.ExecutionId);
            Assert.AreEqual(instance.InstanceId, actual.InstanceId);
            Assert.IsNotNull(actual.ExtensionData);

            // Writing an instance with ExtensionData, then reading it as a more derived type will repopulate extra fields.
            json = JsonConvert.SerializeObject(actual, settings);
            TestOrchestrationInstance actual2 = JsonConvert.DeserializeObject<TestOrchestrationInstance>(json, settings);

            Assert.AreEqual(instance.ExecutionId, actual2.ExecutionId);
            Assert.AreEqual(instance.InstanceId, actual2.InstanceId);
            Assert.AreEqual(instance.Extra, actual2.Extra);
        }

        [DataContract(
            Name = "OrchestrationInstance",
            Namespace = "http://schemas.datacontract.org/2004/07/DurableTask.Core")]
        public class TestOrchestrationInstance : OrchestrationInstance
        {
            [DataMember]
            public string Extra { get; set; }
        }
    }
}
