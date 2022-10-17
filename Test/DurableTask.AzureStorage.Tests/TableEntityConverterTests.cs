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
    using System;
    using System.Linq;
    using System.Runtime.Serialization;
    using Azure.Data.Tables;
    using DurableTask.AzureStorage.Tracking;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class TableEntityConverterTests
    {
        [TestMethod]
        public void DeserializeNonNull()
        {
            DateTime utcNow = DateTime.UtcNow;
            DateTimeOffset utcNowOffset = DateTimeOffset.UtcNow;
            Guid g1 = Guid.NewGuid();
            Guid g2 = Guid.NewGuid();
            var entity = new TableEntity
            {
                [nameof(Example.EnumField)] = ExampleEnum.B.ToString("G"),
                [nameof(Example.NullableEnumProperty)] = ExampleEnum.C.ToString("G"),
                [nameof(Example.StringProperty)] = "Hello World",
                [nameof(Example.BinaryDataField)] = new BinaryData(new byte[] { 1, 2, 3, 4, 5 }),
                [nameof(Example.BinaryProperty)] = new byte[] { 6, 7, 8 },
                [nameof(Example.BoolProperty)] = true,
                [nameof(Example.NullableBoolProperty)] = true,
                ["_Timestamp"] = utcNow,
                [nameof(Example.NullableDateTimeField)] = utcNow.AddDays(-1),
                [nameof(Example.DateTimeOffsetProperty)] = utcNowOffset.AddYears(-5),
                [nameof(Example.NullableDateTimeOffsetProperty)] = utcNowOffset.AddMonths(-2),
                ["Overridden"] = 1.234D,
                [nameof(Example.NullableDoubleProperty)] = 56.789D,
                [nameof(Example.GuidProperty)] = g1,
                [nameof(Example.NullableGuidField)] = g2,
                [nameof(Example.IntField)] = 42,
                [nameof(Example.NullableIntField)] = 10162022,
                [nameof(Example.LongField)] = -2L,
                [nameof(Example.NullableLongProperty)] = long.MaxValue,
                [nameof(Example.UnsupportedProperty)] = Utils.SerializeToJson((short)7),
                [nameof(Example.ObjectProperty)] = Utils.SerializeToJson(new Nested { Phrase = "Hello again", Number = -42 }),
            };

            Example actual = (Example)TableEntityConverter.Deserialize(entity, typeof(Example));

            Assert.AreEqual(ExampleEnum.B, actual.EnumField);
            Assert.AreEqual(ExampleEnum.C, actual.NullableEnumProperty);
            Assert.AreEqual("Hello World", actual.StringProperty);
            Assert.IsTrue(actual.BinaryDataField.ToArray().SequenceEqual(new byte[] { 1, 2, 3, 4, 5 }));
            Assert.IsTrue(actual.BinaryProperty.SequenceEqual(new byte[] { 6, 7, 8 }));
            Assert.IsTrue(actual.BoolProperty);
            Assert.IsTrue(actual.NullableBoolProperty.Value);
            Assert.AreEqual(utcNow, actual.Timestamp);
            Assert.AreEqual(utcNow.AddDays(-1), actual.NullableDateTimeField);
            Assert.AreEqual(utcNowOffset.AddYears(-5), actual.DateTimeOffsetProperty);
            Assert.AreEqual(utcNowOffset.AddMonths(-2), actual.NullableDateTimeOffsetProperty);
            Assert.AreEqual(1.234D, actual.DoubleField);
            Assert.AreEqual(56.789D, actual.NullableDoubleProperty);
            Assert.AreEqual(g1, actual.GuidProperty);
            Assert.AreEqual(g2, actual.NullableGuidField);
            Assert.AreEqual(42, actual.IntField);
            Assert.AreEqual(10162022, actual.NullableIntField);
            Assert.AreEqual(-2L, actual.LongField);
            Assert.AreEqual(long.MaxValue, actual.NullableLongProperty);
            Assert.AreEqual((short)7, actual.UnsupportedProperty);
            Assert.AreEqual("Hello again", actual.ObjectProperty.Phrase);
            Assert.AreEqual(-42, actual.ObjectProperty.Number);
        }

        [TestMethod]
        public void DeserializeNull()
        {
            // We'll both set null values and leave some values unspecified
            var entity = new TableEntity
            {
                [nameof(Example.NullableEnumProperty)] = null,
                [nameof(Example.BinaryProperty)] = null,
                [nameof(Example.NullableBoolProperty)] = null,
                [nameof(Example.NullableDateTimeOffsetProperty)] = null,
                [nameof(Example.NullableGuidField)] = null,
                [nameof(Example.NullableIntField)] = null,
                [nameof(Example.ObjectProperty)] = null,
            };

            Example actual = (Example)TableEntityConverter.Deserialize(entity, typeof(Example));

            Assert.AreEqual(ExampleEnum.A, actual.EnumField);
            Assert.IsNull(actual.NullableEnumProperty);
            Assert.IsNull(actual.StringProperty);
            Assert.IsNull(actual.BinaryDataField);
            Assert.IsNull(actual.BinaryProperty);
            Assert.AreEqual(default(bool), actual.BoolProperty);
            Assert.IsNull(actual.NullableBoolProperty);
            Assert.AreEqual(default(DateTime), actual.Timestamp);
            Assert.IsNull(actual.NullableDateTimeField);
            Assert.AreEqual(default(DateTimeOffset), actual.DateTimeOffsetProperty);
            Assert.IsNull(actual.NullableDateTimeOffsetProperty);
            Assert.AreEqual(default(double), actual.DoubleField);
            Assert.IsNull(actual.NullableDoubleProperty);
            Assert.AreEqual(default(Guid), actual.GuidProperty);
            Assert.IsNull(actual.NullableGuidField);
            Assert.AreEqual(default(int), actual.IntField);
            Assert.IsNull(actual.NullableIntField);
            Assert.AreEqual(default(long), actual.LongField);
            Assert.IsNull(actual.NullableLongProperty);
            Assert.AreEqual(default(short), actual.UnsupportedProperty);
            Assert.IsNull(actual.ObjectProperty);
        }

        [TestMethod]
        public void SerializeNonNull()
        {
            var expected = new Example(default)
            {
                EnumField = ExampleEnum.B,
                NullableEnumProperty = ExampleEnum.C,
                StringProperty = "Hello World",
                BinaryDataField = new BinaryData(new byte[] { 1, 2, 3, 4, 5 }),
                BinaryProperty = new byte[] { 6, 7, 8 },
                BoolProperty = true,
                NullableBoolProperty = true,
                Timestamp = DateTime.UtcNow,
                NullableDateTimeField = DateTime.UtcNow.AddDays(-1),
                DateTimeOffsetProperty = DateTimeOffset.UtcNow.AddYears(-5),
                NullableDateTimeOffsetProperty = DateTimeOffset.UtcNow.AddMonths(-2),
                DoubleField = 1.234,
                NullableDoubleProperty = 56.789,
                GuidProperty = Guid.NewGuid(),
                NullableGuidField = Guid.NewGuid(),
                IntField = 42,
                NullableIntField = 10162022,
                LongField = -2,
                NullableLongProperty = long.MaxValue,
                Skipped = "Not Used",
                UnsupportedProperty = 7,
                ObjectProperty = new Nested
                {
                    Phrase = "Hello again",
                    Number = -42,
                },
            };

            AssertEntity(expected, TableEntityConverter.Serialize(expected));
        }

        [TestMethod]
        public void SerializeNull()
        {
            // Of course, these null values are the defaults,
            // but we'll set them explicitly to illustrate the purpose of the test
            var expected = new Example(default)
            {
                NullableEnumProperty = null,
                StringProperty = null,
                BinaryDataField = null,
                BinaryProperty = null,
                NullableBoolProperty = null,
                NullableDateTimeField = null,
                NullableDateTimeOffsetProperty = null,
                NullableDoubleProperty = null,
                NullableGuidField = null,
                NullableIntField = null,
                NullableLongProperty = null,
                Skipped = "Not Used",
                ObjectProperty = null,
            };

            AssertEntity(expected, TableEntityConverter.Serialize(expected));
        }

        static void AssertEntity(Example expected, TableEntity actual)
        {
            Assert.AreEqual(expected.EnumField.ToString(), actual.GetString(nameof(Example.EnumField)));
            Assert.AreEqual(expected.NullableEnumProperty?.ToString(), actual.GetString(nameof(Example.NullableEnumProperty)));
            Assert.AreEqual(expected.StringProperty, actual.GetString(nameof(Example.StringProperty)));
            Assert.AreEqual(expected.BoolProperty, actual.GetBoolean(nameof(Example.BoolProperty)));
            Assert.AreEqual(expected.NullableBoolProperty, actual.GetBoolean(nameof(Example.NullableBoolProperty)));
            Assert.AreEqual(expected.Timestamp, actual.GetDateTime("_Timestamp"));
            Assert.AreEqual(expected.NullableDateTimeField, actual.GetDateTime(nameof(Example.NullableDateTimeField)));
            Assert.AreEqual(expected.DateTimeOffsetProperty, actual.GetDateTimeOffset(nameof(Example.DateTimeOffsetProperty)));
            Assert.AreEqual(expected.NullableDateTimeOffsetProperty, actual.GetDateTimeOffset(nameof(Example.NullableDateTimeOffsetProperty)));
            Assert.AreEqual(expected.DoubleField, actual.GetDouble("Overridden"));
            Assert.AreEqual(expected.NullableDoubleProperty, actual.GetDouble(nameof(Example.NullableDoubleProperty)));
            Assert.AreEqual(expected.GuidProperty, actual.GetGuid(nameof(Example.GuidProperty)));
            Assert.AreEqual(expected.NullableGuidField, actual.GetGuid(nameof(Example.NullableGuidField)));
            Assert.AreEqual(expected.IntField, actual.GetInt32(nameof(Example.IntField)));
            Assert.AreEqual(expected.NullableIntField, actual.GetInt32(nameof(Example.NullableIntField)));
            Assert.AreEqual(expected.LongField, actual.GetInt64(nameof(Example.LongField)));
            Assert.AreEqual(expected.NullableLongProperty, actual.GetInt64(nameof(Example.NullableLongProperty)));
            Assert.IsFalse(actual.ContainsKey(nameof(expected.Skipped)));
            Assert.AreEqual(Utils.SerializeToJson(expected.UnsupportedProperty), actual.GetString(nameof(Example.UnsupportedProperty)));
            Assert.AreEqual(Utils.SerializeToJson(expected.ObjectProperty), actual.GetString(nameof(Example.ObjectProperty)));

            if (expected.BinaryDataField == null)
            {
                Assert.IsNull(actual.GetBinaryData(nameof(Example.BinaryDataField)));
            }
            else
            {
                Assert.IsTrue(expected.BinaryDataField.ToArray().SequenceEqual(actual.GetBinaryData(nameof(Example.BinaryDataField)).ToArray()));
            }
            if (expected.BinaryProperty == null)
            {
                Assert.IsNull(actual.GetBinary(nameof(Example.BinaryProperty)));
            }
            else
            {
                Assert.IsTrue(expected.BinaryProperty.SequenceEqual(actual.GetBinary(nameof(Example.BinaryProperty))));
            }
        }

        [DataContract]
        sealed class Example
        {
            [DataMember]
            public ExampleEnum EnumField;

            [DataMember]
            public ExampleEnum? NullableEnumProperty { get; set; }

            [DataMember]
            public string StringProperty { get; set; }

            [DataMember]
            public BinaryData BinaryDataField;

            [DataMember]
            internal byte[] BinaryProperty { get; set; }

            [DataMember]
            public bool BoolProperty { get; set; }

            [DataMember]
            public bool? NullableBoolProperty { get; set; }

            [DataMember]
            public DateTime Timestamp { get; set; } // This will be renamed

            [DataMember]
            internal DateTime? NullableDateTimeField;

            [DataMember]
            public DateTimeOffset DateTimeOffsetProperty { get; set; }

            [DataMember]
            public DateTimeOffset? NullableDateTimeOffsetProperty { get; set; }

            [DataMember(Name = "Overridden")]
            internal double DoubleField;

            [DataMember]
            internal double? NullableDoubleProperty { get; set; }

            [DataMember]
            public Guid GuidProperty { get; set; }

            [DataMember]
            public Guid? NullableGuidField;

            [DataMember]
            public int IntField;

            [DataMember]
            internal int? NullableIntField;

            [DataMember]
            public long LongField;

            [DataMember]
            internal long? NullableLongProperty { get; set; }

            public string Skipped { get; set; }

            [DataMember]
            public short UnsupportedProperty { get; set; }

            [DataMember]
            internal Nested ObjectProperty { get; set; }

            public Example(int intField)
            {
                this.IntField = intField;
            }
        }

        public sealed class Nested
        {
            public string Phrase { get; set; }

            public int Number { get; set; }
        }

        private enum ExampleEnum
        {
            A,
            B,
            C,
        }
    }
}
