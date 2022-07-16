namespace DurableTask.AzureStorage.Tests
{
    using System;
    using System.Runtime.Serialization;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;

    [TestClass]
    public class DataContractJsonConverterTests
    {
        static readonly JsonSerializerSettings SerializerSettings = new JsonSerializerSettings
        {
            Converters =
            {
                new DataContractJsonConverter(),
            }
        };

        [TestMethod]
        public void ReadWrite_Null_Succeeds()
        {
            var container = new Container();
            string json = Serialize(container);
            var actual = Deserialize<Container>(json);
            Assert.IsNotNull(actual);
            Assert.IsNull(actual.Instance);
        }

        [TestMethod]
        public void ReadWrite_OrchestrationInstance_Succeeds()
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

            VerifyEqual(instance, actual);

            // Writing an instance with ExtensionData, then reading it as a more derived type will repopulate extra fields.
            json = Serialize(actual);
            TestOrchestrationInstance actual2 = Deserialize<TestOrchestrationInstance>(json);
            VerifyEqual(instance, actual2);
            Assert.AreEqual(instance.Extra, actual2.Extra);
        }

        [TestMethod]
        public void ReadWrite_TaskMessage_Succeeds()
        {
            var instance = new TaskMessage
            {
                Event = new GenericEvent(1, "Some Data"),
                SequenceNumber = 10,
                OrchestrationInstance = new OrchestrationInstance
                {
                    ExecutionId = Guid.NewGuid().ToString(),
                    InstanceId = Guid.NewGuid().ToString(),
                },
            };

            // Writing a more derived type, and then reading it will populate ExtensionData.
            string json = Serialize(instance);
            TaskMessage actual = Deserialize<TaskMessage>(json);
            VerifyEqual(instance, actual);
        }

        static void VerifyEqual(TaskMessage expected, TaskMessage actual)
        {
            Assert.IsNotNull(actual);
            Assert.AreEqual(expected.SequenceNumber, actual.SequenceNumber);
            VerifyEqual(expected.OrchestrationInstance, actual.OrchestrationInstance);
            VerifyEqual(expected.Event, actual.Event);
            Assert.IsNotNull(actual.ExtensionData);
        }

        static void VerifyEqual(OrchestrationInstance expected, OrchestrationInstance actual)
        {
            Assert.IsNotNull(actual);
            Assert.AreEqual(expected.ExecutionId, actual.ExecutionId);
            Assert.AreEqual(expected.InstanceId, actual.InstanceId);
            Assert.IsNotNull(actual.ExtensionData);
        }

        static void VerifyEqual(HistoryEvent expected, HistoryEvent actual)
        {
            Assert.IsNotNull(actual);
            Assert.AreEqual(expected.GetType(), actual.GetType());
            Assert.AreEqual(expected.EventId, actual.EventId);
            Assert.AreEqual(expected.EventType, actual.EventType);
            Assert.AreEqual(expected.IsPlayed, actual.IsPlayed);
            Assert.AreEqual(expected.Timestamp, actual.Timestamp);
            Assert.IsNotNull(actual.ExtensionData);
        }

        static string Serialize(object value) => JsonConvert.SerializeObject(value, SerializerSettings);

        static T Deserialize<T>(string json) => JsonConvert.DeserializeObject<T>(json, SerializerSettings);

        [DataContract(
            Name = "OrchestrationInstance",
            Namespace = "http://schemas.datacontract.org/2004/07/DurableTask.Core")]
        class TestOrchestrationInstance : OrchestrationInstance
        {
            [DataMember]
            public string Extra { get; set; }
        }

        class Container
        {
            public OrchestrationInstance Instance { get; set; }
        }
    }
}
