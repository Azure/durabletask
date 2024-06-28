using DurableTask.Core;
using DurableTask.Core.History;
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
            TypeNameHandling = TypeNameHandling.Objects,
            Converters =
            {
                new DataContractJsonConverter()
                {
                    alternativeSerializer = JsonSerializer.Create(new JsonSerializerSettings
                    {
                        TypeNameHandling = TypeNameHandling.Objects,
                    }),
                },
            }
        };

        [TestMethod]
        public void Fallback_Succeeds()
        {
            var messageManager = new MessageManager(
                new AzureStorageOrchestrationServiceSettings() { UseDataContractSerialization = true },
                null,
                "test");
            string json = @"{""$type"":""DurableTask.AzureStorage.MessageData"",""ActivityId"":""03d10904-8a30-4bc5-9659-3c2c46b9435c"",""TaskMessage"":{""Event"":{""__type"":""TimerFiredEvent:#DurableTask.Core.History"",""EventId"":-1,""EventType"":11,""IsPlayed"":false,""Timestamp"":""2024-03-14T16:00:05.1285428Z"",""FireAt"":""2024-03-14T19:00:05.0563363Z"",""TimerId"":1},""OrchestrationInstance"":{""ExecutionId"":""8c0384066f414e1e9f4f8af94f9cc03f"",""InstanceId"":""8f30203236d34f8ea5eeff5428eb5ee7:20""},""SequenceNumber"":0},""SequenceNumber"":147825,""Episode"":1,""Sender"":{""ExecutionId"":""8c0384066f414e1e9f4f8af94f9cc03f"",""InstanceId"":""8f30203236d34f8ea5eeff5428eb5ee7:20""},""SerializableTraceContext"":""{\""$id\"":\""1\"",\""$type\"":\""DurableTask.Core.W3CTraceContext, DurableTask.Core\"",\""TraceParent\"":\""00-8fdaec3721ee2f7e0e43b5be5b22e54f-264d84ac14ac0835-00\"",\""TraceState\"":\""corId=8bb00592-7083-4190-8a20-98703183e8ca\"",\""ParentSpanId\"":\""abcd4f1b232271e8\"",\""StartTime\"":\""2024-03-14T16:00:05.1286681+00:00\"",\""TelemetryType\"":\""Dependency\"",\""OrchestrationTraceContexts\"":[{\""$ref\"":\""1\""},{\""$id\"":\""2\"",\""$type\"":\""DurableTask.Core.W3CTraceContext, DurableTask.Core\"",\""TraceParent\"":\""00-8fdaec3721ee2f7e0e43b5be5b22e54f-abcd4f1b232271e8-00\"",\""TraceState\"":\""corId=8bb00592-7083-4190-8a20-98703183e8ca\"",\""ParentSpanId\"":\""19d9c9a2ac642d79\"",\""StartTime\"":\""2024-03-14T16:00:04.7358608+00:00\"",\""TelemetryType\"":\""Request\"",\""OrchestrationTraceContexts\"":[{\""$ref\"":\""2\""},{\""$id\"":\""3\"",\""$type\"":\""DurableTask.Core.W3CTraceContext, DurableTask.Core\"",\""TraceParent\"":\""00-8fdaec3721ee2f7e0e43b5be5b22e54f-19d9c9a2ac642d79-00\"",\""TraceState\"":\""corId=8bb00592-7083-4190-8a20-98703183e8ca\"",\""ParentSpanId\"":\""586d2cfbe8ad7fb6\"",\""StartTime\"":\""2024-03-14T16:00:03.8475412+00:00\"",\""TelemetryType\"":\""Dependency\"",\""OrchestrationTraceContexts\"":[{\""$ref\"":\""3\""},{\""$id\"":\""4\"",\""$type\"":\""DurableTask.Core.W3CTraceContext, DurableTask.Core\"",\""TraceParent\"":\""00-8fdaec3721ee2f7e0e43b5be5b22e54f-586d2cfbe8ad7fb6-00\"",\""TraceState\"":\""corId=8bb00592-7083-4190-8a20-98703183e8ca\"",\""ParentSpanId\"":\""1402edc2597b9617\"",\""StartTime\"":\""2024-03-14T16:00:01.7224413+00:00\"",\""TelemetryType\"":\""Request\"",\""OrchestrationTraceContexts\"":[{\""$id\"":\""5\"",\""$type\"":\""DurableTask.Core.W3CTraceContext, DurableTask.Core\"",\""TraceParent\"":\""00-8fdaec3721ee2f7e0e43b5be5b22e54f-22a551c6fd5d3acd-00\"",\""TraceState\"":\""corId=8bb00592-7083-4190-8a20-98703183e8ca\"",\""ParentSpanId\"":\""9a882fa89395da4f\"",\""StartTime\"":\""2024-03-14T15:56:00.8004397+00:00\"",\""TelemetryType\"":\""Request\"",\""OrchestrationTraceContexts\"":[{\""$ref\"":\""5\""}],\""OperationName\"":\""DtOrchestrator\""},{\""$id\"":\""6\"",\""$type\"":\""DurableTask.Core.W3CTraceContext, DurableTask.Core\"",\""TraceParent\"":\""00-8fdaec3721ee2f7e0e43b5be5b22e54f-e331e54d39cad3c4-00\"",\""TraceState\"":\""corId=8bb00592-7083-4190-8a20-98703183e8ca\"",\""ParentSpanId\"":\""22a551c6fd5d3acd\"",\""StartTime\"":\""2024-03-14T15:56:01.3294411+00:00\"",\""TelemetryType\"":\""Dependency\"",\""OrchestrationTraceContexts\"":[{\""$ref\"":\""5\""},{\""$ref\"":\""6\""}],\""OperationName\"":\""DtOrchestrator Microsoft.LabServices.ManagedLabs.Application.Schedules.RunScheduleActionOrchestration+Handler\""},{\""$id\"":\""7\"",\""$type\"":\""DurableTask.Core.W3CTraceContext, DurableTask.Core\"",\""TraceParent\"":\""00-8fdaec3721ee2f7e0e43b5be5b22e54f-179a6ba5961635d7-00\"",\""TraceState\"":\""corId=8bb00592-7083-4190-8a20-98703183e8ca\"",\""ParentSpanId\"":\""e331e54d39cad3c4\"",\""StartTime\"":\""2024-03-14T15:56:01.4212406+00:00\"",\""TelemetryType\"":\""Request\"",\""OrchestrationTraceContexts\"":[{\""$ref\"":\""7\""},{\""$ref\"":\""6\""},{\""$ref\"":\""5\""}],\""OperationName\"":\""DtOrchestrator\""},{\""$id\"":\""8\"",\""$type\"":\""DurableTask.Core.W3CTraceContext, DurableTask.Core\"",\""TraceParent\"":\""00-8fdaec3721ee2f7e0e43b5be5b22e54f-1402edc2597b9617-00\"",\""TraceState\"":\""corId=8bb00592-7083-4190-8a20-98703183e8ca\"",\""ParentSpanId\"":\""179a6ba5961635d7\"",\""StartTime\"":\""2024-03-14T16:00:01.3381712+00:00\"",\""TelemetryType\"":\""Dependency\"",\""OrchestrationTraceContexts\"":[{\""$ref\"":\""5\""},{\""$ref\"":\""6\""},{\""$ref\"":\""7\""},{\""$ref\"":\""8\""}],\""OperationName\"":\""DtOrchestrator Microsoft.LabServices.ManagedLabs.Application.Labs.NotifyLabVmTrackersOrchestration+Handler\""},{\""$ref\"":\""4\""}],\""OperationName\"":\""DtOrchestrator\""},{\""$ref\"":\""8\""},{\""$ref\"":\""7\""},{\""$ref\"":\""6\""},{\""$ref\"":\""5\""}],\""OperationName\"":\""DtOrchestrator Microsoft.LabServices.ManagedLabs.Application.VirtualMachines.NotifyVmTrackerOrchestration+Handler\""},{\""$ref\"":\""4\""},{\""$ref\"":\""8\""},{\""$ref\"":\""7\""},{\""$ref\"":\""6\""},{\""$ref\"":\""5\""}],\""OperationName\"":\""DtOrchestrator\""},{\""$ref\"":\""3\""},{\""$ref\"":\""4\""},{\""$ref\"":\""8\""},{\""$ref\"":\""7\""},{\""$ref\"":\""6\""},{\""$ref\"":\""5\""}],\""OperationName\"":\""DtOrchestrator outbound\""}""}";

            object obj = messageManager.DeserializeMessageData(json);
        }

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

        private static void VerifyEqual(TaskMessage expected, TaskMessage actual)
        {
            Assert.IsNotNull(actual);
            Assert.AreEqual(expected.SequenceNumber, actual.SequenceNumber);
            VerifyEqual(expected.OrchestrationInstance, actual.OrchestrationInstance);
            VerifyEqual(expected.Event, actual.Event);
            Assert.IsNotNull(actual.ExtensionData);
        }

        private static void VerifyEqual(OrchestrationInstance expected, OrchestrationInstance actual)
        {
            Assert.IsNotNull(actual);
            Assert.AreEqual(expected.ExecutionId, actual.ExecutionId);
            Assert.AreEqual(expected.InstanceId, actual.InstanceId);
            Assert.IsNotNull(actual.ExtensionData);
        }

        private static void VerifyEqual(HistoryEvent expected, HistoryEvent actual)
        {
            Assert.IsNotNull(actual);
            Assert.AreEqual(expected.GetType(), actual.GetType());
            Assert.AreEqual(expected.EventId, actual.EventId);
            Assert.AreEqual(expected.EventType, actual.EventType);
            Assert.AreEqual(expected.IsPlayed, actual.IsPlayed);
            Assert.AreEqual(expected.Timestamp, actual.Timestamp);
            Assert.IsNotNull(actual.ExtensionData);
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
