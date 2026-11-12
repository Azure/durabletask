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

namespace DurableTask.Core.Tests
{
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Xml;
    using System.Xml.Linq;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class FailureDetailsSerializationTests
    {
        [TestMethod]
        public void DataContractSerializer_TaskFailedEventWithoutPropertiesField_Deserializes()
        {
            // Simulates history persisted by DurableTask.Core versions that predate FailureDetails.Properties.
            var innerFailure = new FailureDetails("System.TimeoutException", "inner", "inner stack", innerFailure: null, isNonRetriable: false);
            var failureDetails = new FailureDetails("System.InvalidOperationException", "outer", "outer stack", innerFailure, isNonRetriable: true);
            var original = new TaskFailedEvent(eventId: 5, taskScheduledId: 3, "reason", "details", failureDetails);

            string legacyXml = RemovePropertiesElements(Serialize(original));
            Assert.IsFalse(legacyXml.Contains("Properties"), "The legacy payload should not contain the Properties field.");

            TaskFailedEvent result = Deserialize<TaskFailedEvent>(legacyXml);

            Assert.AreEqual(3, result.TaskScheduledId);
            Assert.IsNotNull(result.FailureDetails);
            Assert.AreEqual("System.InvalidOperationException", result.FailureDetails.ErrorType);
            Assert.AreEqual("outer", result.FailureDetails.ErrorMessage);
            Assert.AreEqual("outer stack", result.FailureDetails.StackTrace);
            Assert.IsTrue(result.FailureDetails.IsNonRetriable);
            Assert.IsNull(result.FailureDetails.Properties);
            Assert.IsNotNull(result.FailureDetails.InnerFailure);
            Assert.AreEqual(innerFailure.ErrorType, result.FailureDetails.InnerFailure.ErrorType);
            Assert.AreEqual(innerFailure.ErrorMessage, result.FailureDetails.InnerFailure.ErrorMessage);
            Assert.IsNull(result.FailureDetails.InnerFailure.Properties);
        }

        [TestMethod]
        public void DataContractSerializer_FailureDetailsWithProperties_RoundTrips()
        {
            var properties = new Dictionary<string, object?> { ["Code"] = "E42", ["Missing"] = null };
            var original = new FailureDetails("System.Exception", "message", "stack", innerFailure: null, isNonRetriable: false, properties);

            FailureDetails result = Deserialize<FailureDetails>(Serialize(original));

            Assert.IsNotNull(result.Properties);
            Assert.AreEqual("E42", result.Properties["Code"]);
            Assert.IsTrue(result.Properties.ContainsKey("Missing"));
            Assert.IsNull(result.Properties["Missing"]);
        }

        static string Serialize<T>(T value)
        {
            var serializer = new DataContractSerializer(typeof(T));
            using var stream = new MemoryStream();
            serializer.WriteObject(stream, value);
            stream.Position = 0;
            using var reader = new StreamReader(stream);
            return reader.ReadToEnd();
        }

        static T Deserialize<T>(string xml)
        {
            var serializer = new DataContractSerializer(typeof(T));
            using var stringReader = new StringReader(xml);
            using var xmlReader = XmlReader.Create(stringReader);
            return (T)serializer.ReadObject(xmlReader)!;
        }

        static string RemovePropertiesElements(string xml)
        {
            XDocument document = XDocument.Parse(xml);
            document.Descendants()
                .Where(e => e.Name.LocalName.Contains("Properties"))
                .ToList()
                .ForEach(e => e.Remove());
            return document.ToString(SaveOptions.DisableFormatting);
        }
    }
}
