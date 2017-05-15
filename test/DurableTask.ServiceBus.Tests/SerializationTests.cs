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
    using DurableTask.Core.History;
    using DurableTask.Core.Serializing;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class SerializationTests
    {
        TaskHubClient client;
        TaskHubWorker taskHub;

        [TestInitialize]
        public void TestInitialize()
        {
            client = TestHelpers.CreateTaskHubClient();

            taskHub = TestHelpers.CreateTaskHub();
            taskHub.orchestrationService.CreateAsync(true).Wait();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            taskHub.StopAsync(true).Wait();
            taskHub.orchestrationService.DeleteAsync(true).Wait();
        }

        #region Interface based activity serialization tests

        public enum TestEnum
        {
            Val1,
            Val2
        }

        [TestMethod]
        public void DataConverterDeserializationTest()
        {
            var dataConverter = new JsonDataConverter();

            string serializedState = File.ReadAllText(@"TestData\SerializedExecutionStartedEvent.json");
            var startedEvent = dataConverter.Deserialize<ExecutionStartedEvent>(serializedState);
            Assert.IsNotNull(startedEvent);

            serializedState = File.ReadAllText(@"TestData\SerializedOrchestrationState.json");
            var runtimeState = dataConverter.Deserialize<OrchestrationRuntimeState>(serializedState);
            Assert.IsNotNull(runtimeState);

            serializedState = File.ReadAllText(@"TestData\SerializedOrchestrationStateWithTags.json");
            runtimeState = dataConverter.Deserialize<OrchestrationRuntimeState>(serializedState);
            Assert.IsNotNull(runtimeState);
        }

        [TestMethod]
        public async Task PrimitiveTypeActivitiesSerializationTest()
        {
            await taskHub.AddTaskOrchestrations(typeof (PrimitiveTypeActivitiesOrchestration))
                .AddTaskActivitiesFromInterface<IPrimitiveTypeActivities>(new PrimitiveTypeActivities())
                .StartAsync();

            var input = new PrimitiveTypeOrchestrationInput
            {
                Byte = 10,
                SByte = -10,
                Integer = -2000000000,
                UInteger = 4000000000,
                Short = -32000,
                UShort = 65000,
                Long = -9000000000000000000,
                ULong = 18000000000000000000,
                Float = 3.5F,
                Double = 3D,
                Char = 'A',
                Bool = true,
                Str = "Hello",
                Decimal = 300.5m,
                DateTime = DateTime.Now,
                TimeSpan = TimeSpan.FromDays(2),
                Enum = TestEnum.Val2,
            };
            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(
                typeof (PrimitiveTypeActivitiesOrchestration), input);

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 120);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 120));

            Assert.AreEqual(input.Byte, PrimitiveTypeActivitiesOrchestration.Byte);
            Assert.AreEqual(input.SByte, PrimitiveTypeActivitiesOrchestration.SByte);
            Assert.AreEqual(input.Integer, PrimitiveTypeActivitiesOrchestration.Integer);
            Assert.AreEqual(input.UInteger, PrimitiveTypeActivitiesOrchestration.UInteger);
            Assert.AreEqual(input.Short, PrimitiveTypeActivitiesOrchestration.Short);
            Assert.AreEqual(input.UShort, PrimitiveTypeActivitiesOrchestration.UShort);
            Assert.AreEqual(input.Long, PrimitiveTypeActivitiesOrchestration.Long);
            Assert.AreEqual(input.ULong, PrimitiveTypeActivitiesOrchestration.ULong);
            Assert.AreEqual(input.Float, PrimitiveTypeActivitiesOrchestration.Float);
            Assert.AreEqual(input.Double, PrimitiveTypeActivitiesOrchestration.Double);
            Assert.AreEqual(input.Char, PrimitiveTypeActivitiesOrchestration.Char);
            Assert.AreEqual(input.Bool, PrimitiveTypeActivitiesOrchestration.Bool);
            Assert.AreEqual(input.Str, PrimitiveTypeActivitiesOrchestration.Str);
            Assert.AreEqual(input.Decimal, PrimitiveTypeActivitiesOrchestration.Decimal);
            Assert.AreEqual(input.DateTime, PrimitiveTypeActivitiesOrchestration.DateTime);
            Assert.AreEqual(input.TimeSpan, PrimitiveTypeActivitiesOrchestration.TimeSpan);
            Assert.AreEqual(input.Enum, PrimitiveTypeActivitiesOrchestration.MyEnum);


            for (int i = 0; i < 2; i++)
            {
                Assert.AreEqual(input.Byte, PrimitiveTypeActivitiesOrchestration.ByteArray[i]);
                Assert.AreEqual(input.SByte, PrimitiveTypeActivitiesOrchestration.SByteArray[i]);
                Assert.AreEqual(input.Integer, PrimitiveTypeActivitiesOrchestration.IntegerArray[i]);
                Assert.AreEqual(input.UInteger, PrimitiveTypeActivitiesOrchestration.UIntegerArray[i]);
                Assert.AreEqual(input.Short, PrimitiveTypeActivitiesOrchestration.ShortArray[i]);
                Assert.AreEqual(input.UShort, PrimitiveTypeActivitiesOrchestration.UShortArray[i]);
                Assert.AreEqual(input.Long, PrimitiveTypeActivitiesOrchestration.LongArray[i]);
                Assert.AreEqual(input.ULong, PrimitiveTypeActivitiesOrchestration.ULongArray[i]);
                Assert.AreEqual(input.Float, PrimitiveTypeActivitiesOrchestration.FloatArray[i]);
                Assert.AreEqual(input.Double, PrimitiveTypeActivitiesOrchestration.DoubleArray[i]);
                Assert.AreEqual(input.Char, PrimitiveTypeActivitiesOrchestration.CharArray[i]);
                Assert.AreEqual(input.Bool, PrimitiveTypeActivitiesOrchestration.BoolArray[i]);
                Assert.AreEqual(input.Str, PrimitiveTypeActivitiesOrchestration.StrArray[i]);
                Assert.AreEqual(input.Decimal, PrimitiveTypeActivitiesOrchestration.DecimalArray[i]);
                Assert.AreEqual(input.DateTime, PrimitiveTypeActivitiesOrchestration.DateTimeArray[i]);
                Assert.AreEqual(input.TimeSpan, PrimitiveTypeActivitiesOrchestration.TimeSpanArray[i]);
                Assert.AreEqual(input.Enum, PrimitiveTypeActivitiesOrchestration.MyEnumArray[i]);
            }
        }

        public interface IPrimitiveTypeActivities
        {
            byte ReturnByte(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f, double d,
                char c, bool bl, string str, decimal dm);

            sbyte ReturnSByte(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f, double d,
                char c, bool bl, string str, decimal dm);

            int ReturnInteger(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f, double d,
                char c, bool bl, string str, decimal dm);

            uint ReturnUInteger(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm);

            short ReturnShort(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f, double d,
                char c, bool bl, string str, decimal dm);

            ushort ReturnUShort(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm);

            long ReturnLong(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f, double d,
                char c, bool bl, string str, decimal dm);

            ulong ReturnULong(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f, double d,
                char c, bool bl, string str, decimal dm);

            float ReturnFloat(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f, double d,
                char c, bool bl, string str, decimal dm);

            double ReturnDouble(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm);

            char ReturnChar(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f, double d,
                char c, bool bl, string str, decimal dm);

            bool ReturnBool(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f, double d,
                char c, bool bl, string str, decimal dm);

            string ReturnString(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm);

            decimal ReturnDecimal(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm);

            DateTime ReturnDateTime(DateTime dateTime);
            TimeSpan ReturnTimeSpan(TimeSpan timeSpan);
            TestEnum ReturnEnum(TestEnum e);

            byte[] ReturnByteArray(byte[] b);
            sbyte[] ReturnSByteArray(sbyte[] sb);
            int[] ReturnIntegerArray(int[] i);
            uint[] ReturnUIntegerArray(uint[] ui);
            short[] ReturnShortArray(short[] s);
            ushort[] ReturnUShortArray(ushort[] us);
            long[] ReturnLongArray(long[] l);
            ulong[] ReturnULongArray(ulong[] ul);
            float[] ReturnFloatArray(float[] f);
            double[] ReturnDoubleArray(double[] d);
            char[] ReturnCharArray(char[] c);
            bool[] ReturnBoolArray(bool[] bl);
            string[] ReturnStrArray(string[] str);
            decimal[] ReturnDecimalArray(decimal[] dm);
            DateTime[] ReturnDateTimeArray(DateTime[] dates);
            TimeSpan[] ReturnTimeSpanArray(TimeSpan[] spans);
            TestEnum[] ReturnEnumArray(TestEnum[] e);
        }

        public interface IPrimitiveTypeActivitiesClient
        {
            Task<byte> ReturnByte(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm);

            Task<sbyte> ReturnSByte(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm);

            Task<int> ReturnInteger(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm);

            Task<uint> ReturnUInteger(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm);

            Task<short> ReturnShort(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm);

            Task<ushort> ReturnUShort(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm);

            Task<long> ReturnLong(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm);

            Task<ulong> ReturnULong(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm);

            Task<float> ReturnFloat(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm);

            Task<double> ReturnDouble(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm);

            Task<char> ReturnChar(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm);

            Task<bool> ReturnBool(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm);

            Task<string> ReturnString(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm);

            Task<decimal> ReturnDecimal(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm);

            Task<DateTime> ReturnDateTime(DateTime dateTime);
            Task<TimeSpan> ReturnTimeSpan(TimeSpan timeSpan);
            Task<TestEnum> ReturnEnum(TestEnum e);
            Task<byte[]> ReturnByteArray(byte[] b);
            Task<sbyte[]> ReturnSByteArray(sbyte[] sb);
            Task<int[]> ReturnIntegerArray(int[] i);
            Task<uint[]> ReturnUIntegerArray(uint[] ui);
            Task<short[]> ReturnShortArray(short[] s);
            Task<ushort[]> ReturnUShortArray(ushort[] us);
            Task<long[]> ReturnLongArray(long[] l);
            Task<ulong[]> ReturnULongArray(ulong[] ul);
            Task<float[]> ReturnFloatArray(float[] f);
            Task<double[]> ReturnDoubleArray(double[] d);
            Task<char[]> ReturnCharArray(char[] c);
            Task<bool[]> ReturnBoolArray(bool[] bl);
            Task<string[]> ReturnStrArray(string[] str);
            Task<decimal[]> ReturnDecimalArray(decimal[] dm);
            Task<DateTime[]> ReturnDateTimeArray(DateTime[] dates);
            Task<TimeSpan[]> ReturnTimeSpanArray(TimeSpan[] spans);
            Task<TestEnum[]> ReturnEnumArray(TestEnum[] e);
        }

        public class PrimitiveTypeActivities : IPrimitiveTypeActivities
        {
            public byte ReturnByte(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm)
            {
                return b;
            }

            public sbyte ReturnSByte(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm)
            {
                return sb;
            }

            public int ReturnInteger(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm)
            {
                return i;
            }

            public uint ReturnUInteger(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm)
            {
                return ui;
            }

            public short ReturnShort(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm)
            {
                return s;
            }

            public ushort ReturnUShort(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm)
            {
                return us;
            }

            public long ReturnLong(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm)
            {
                return l;
            }

            public ulong ReturnULong(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm)
            {
                return ul;
            }

            public float ReturnFloat(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm)
            {
                return f;
            }

            public double ReturnDouble(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm)
            {
                return d;
            }

            public char ReturnChar(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm)
            {
                return c;
            }

            public bool ReturnBool(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm)
            {
                return bl;
            }

            public string ReturnString(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm)
            {
                return str;
            }

            public decimal ReturnDecimal(byte b, sbyte sb, int i, uint ui, short s, ushort us, long l, ulong ul, float f,
                double d, char c, bool bl, string str, decimal dm)
            {
                return dm;
            }


            public DateTime ReturnDateTime(DateTime dateTime)
            {
                return dateTime;
            }

            public TimeSpan ReturnTimeSpan(TimeSpan timeSpan)
            {
                return timeSpan;
            }


            public byte[] ReturnByteArray(byte[] b)
            {
                return b;
            }


            public sbyte[] ReturnSByteArray(sbyte[] sb)
            {
                return sb;
            }

            public int[] ReturnIntegerArray(int[] i)
            {
                return i;
            }

            public uint[] ReturnUIntegerArray(uint[] ui)
            {
                return ui;
            }

            public short[] ReturnShortArray(short[] s)
            {
                return s;
            }

            public ushort[] ReturnUShortArray(ushort[] us)
            {
                return us;
            }

            public long[] ReturnLongArray(long[] l)
            {
                return l;
            }

            public ulong[] ReturnULongArray(ulong[] ul)
            {
                return ul;
            }

            public float[] ReturnFloatArray(float[] f)
            {
                return f;
            }

            public double[] ReturnDoubleArray(double[] d)
            {
                return d;
            }

            public char[] ReturnCharArray(char[] c)
            {
                return c;
            }

            public bool[] ReturnBoolArray(bool[] bl)
            {
                return bl;
            }

            public string[] ReturnStrArray(string[] str)
            {
                return str;
            }

            public decimal[] ReturnDecimalArray(decimal[] dm)
            {
                return dm;
            }

            public DateTime[] ReturnDateTimeArray(DateTime[] dates)
            {
                return dates;
            }

            public TimeSpan[] ReturnTimeSpanArray(TimeSpan[] spans)
            {
                return spans;
            }

            public TestEnum ReturnEnum(TestEnum e)
            {
                return e;
            }

            public TestEnum[] ReturnEnumArray(TestEnum[] e)
            {
                return e;
            }
        }

        sealed class PrimitiveTypeActivitiesOrchestration : TaskOrchestration<string, PrimitiveTypeOrchestrationInput>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static byte Byte { get; set; }
            public static sbyte SByte { get; set; }
            public static int Integer { get; set; }
            public static uint UInteger { get; set; }
            public static short Short { get; set; }
            public static ushort UShort { get; set; }
            public static long Long { get; set; }
            public static ulong ULong { get; set; }
            public static float Float { get; set; }
            public static double Double { get; set; }
            public static char Char { get; set; }
            public static bool Bool { get; set; }
            public static string Str { get; set; }
            public static decimal Decimal { get; set; }
            public static DateTime DateTime { get; set; }
            public static TimeSpan TimeSpan { get; set; }
            public static TestEnum MyEnum { get; set; }

            public static byte[] ByteArray { get; set; }
            public static sbyte[] SByteArray { get; set; }
            public static int[] IntegerArray { get; set; }
            public static uint[] UIntegerArray { get; set; }
            public static short[] ShortArray { get; set; }
            public static ushort[] UShortArray { get; set; }
            public static long[] LongArray { get; set; }
            public static ulong[] ULongArray { get; set; }
            public static float[] FloatArray { get; set; }
            public static double[] DoubleArray { get; set; }
            public static char[] CharArray { get; set; }
            public static bool[] BoolArray { get; set; }
            public static string[] StrArray { get; set; }
            public static decimal[] DecimalArray { get; set; }
            public static DateTime[] DateTimeArray { get; set; }
            public static TimeSpan[] TimeSpanArray { get; set; }
            public static TestEnum[] MyEnumArray { get; set; }

            public override async Task<string> RunTask(OrchestrationContext context,
                PrimitiveTypeOrchestrationInput input)
            {
                var activitiesClient = context.CreateClient<IPrimitiveTypeActivitiesClient>();
                Byte =
                    await
                        activitiesClient.ReturnByte(input.Byte, input.SByte, input.Integer, input.UInteger, input.Short,
                            input.UShort, input.Long, input.ULong, input.Float, input.Double, input.Char, input.Bool,
                            input.Str, input.Decimal);
                SByte =
                    await
                        activitiesClient.ReturnSByte(input.Byte, input.SByte, input.Integer, input.UInteger, input.Short,
                            input.UShort, input.Long, input.ULong, input.Float, input.Double, input.Char, input.Bool,
                            input.Str, input.Decimal);
                Integer =
                    await
                        activitiesClient.ReturnInteger(input.Byte, input.SByte, input.Integer, input.UInteger,
                            input.Short, input.UShort, input.Long, input.ULong, input.Float, input.Double, input.Char,
                            input.Bool, input.Str, input.Decimal);
                UInteger =
                    await
                        activitiesClient.ReturnUInteger(input.Byte, input.SByte, input.Integer, input.UInteger,
                            input.Short, input.UShort, input.Long, input.ULong, input.Float, input.Double, input.Char,
                            input.Bool, input.Str, input.Decimal);
                Short =
                    await
                        activitiesClient.ReturnShort(input.Byte, input.SByte, input.Integer, input.UInteger, input.Short,
                            input.UShort, input.Long, input.ULong, input.Float, input.Double, input.Char, input.Bool,
                            input.Str, input.Decimal);
                UShort =
                    await
                        activitiesClient.ReturnUShort(input.Byte, input.SByte, input.Integer, input.UInteger,
                            input.Short, input.UShort, input.Long, input.ULong, input.Float, input.Double, input.Char,
                            input.Bool, input.Str, input.Decimal);
                Long =
                    await
                        activitiesClient.ReturnLong(input.Byte, input.SByte, input.Integer, input.UInteger, input.Short,
                            input.UShort, input.Long, input.ULong, input.Float, input.Double, input.Char, input.Bool,
                            input.Str, input.Decimal);
                ULong =
                    await
                        activitiesClient.ReturnULong(input.Byte, input.SByte, input.Integer, input.UInteger, input.Short,
                            input.UShort, input.Long, input.ULong, input.Float, input.Double, input.Char, input.Bool,
                            input.Str, input.Decimal);
                Float =
                    await
                        activitiesClient.ReturnFloat(input.Byte, input.SByte, input.Integer, input.UInteger, input.Short,
                            input.UShort, input.Long, input.ULong, input.Float, input.Double, input.Char, input.Bool,
                            input.Str, input.Decimal);
                Double =
                    await
                        activitiesClient.ReturnDouble(input.Byte, input.SByte, input.Integer, input.UInteger,
                            input.Short, input.UShort, input.Long, input.ULong, input.Float, input.Double, input.Char,
                            input.Bool, input.Str, input.Decimal);
                Char =
                    await
                        activitiesClient.ReturnChar(input.Byte, input.SByte, input.Integer, input.UInteger, input.Short,
                            input.UShort, input.Long, input.ULong, input.Float, input.Double, input.Char, input.Bool,
                            input.Str, input.Decimal);
                Bool =
                    await
                        activitiesClient.ReturnBool(input.Byte, input.SByte, input.Integer, input.UInteger, input.Short,
                            input.UShort, input.Long, input.ULong, input.Float, input.Double, input.Char, input.Bool,
                            input.Str, input.Decimal);
                Str =
                    await
                        activitiesClient.ReturnString(input.Byte, input.SByte, input.Integer, input.UInteger,
                            input.Short, input.UShort, input.Long, input.ULong, input.Float, input.Double, input.Char,
                            input.Bool, input.Str, input.Decimal);
                Decimal =
                    await
                        activitiesClient.ReturnDecimal(input.Byte, input.SByte, input.Integer, input.UInteger,
                            input.Short, input.UShort, input.Long, input.ULong, input.Float, input.Double, input.Char,
                            input.Bool, input.Str, input.Decimal);
                DateTime = await activitiesClient.ReturnDateTime(input.DateTime);
                TimeSpan = await activitiesClient.ReturnTimeSpan(input.TimeSpan);
                MyEnum = await activitiesClient.ReturnEnum(input.Enum);
                ByteArray = await activitiesClient.ReturnByteArray(new[] {input.Byte, input.Byte});
                SByteArray = await activitiesClient.ReturnSByteArray(new[] {input.SByte, input.SByte});
                IntegerArray = await activitiesClient.ReturnIntegerArray(new[] {input.Integer, input.Integer});
                UIntegerArray = await activitiesClient.ReturnUIntegerArray(new[] {input.UInteger, input.UInteger});
                ShortArray = await activitiesClient.ReturnShortArray(new[] {input.Short, input.Short});
                UShortArray = await activitiesClient.ReturnUShortArray(new[] {input.UShort, input.UShort});
                LongArray = await activitiesClient.ReturnLongArray(new[] {input.Long, input.Long});
                ULongArray = await activitiesClient.ReturnULongArray(new[] {input.ULong, input.ULong});
                FloatArray = await activitiesClient.ReturnFloatArray(new[] {input.Float, input.Float});
                DoubleArray = await activitiesClient.ReturnDoubleArray(new[] {input.Double, input.Double});
                CharArray = await activitiesClient.ReturnCharArray(new[] {input.Char, input.Char});
                BoolArray = await activitiesClient.ReturnBoolArray(new[] {input.Bool, input.Bool});
                StrArray = await activitiesClient.ReturnStrArray(new[] {input.Str, input.Str});
                DecimalArray = await activitiesClient.ReturnDecimalArray(new[] {input.Decimal, input.Decimal});
                DateTimeArray = await activitiesClient.ReturnDateTimeArray(new[] {input.DateTime, input.DateTime});
                TimeSpanArray = await activitiesClient.ReturnTimeSpanArray(new[] {input.TimeSpan, input.TimeSpan});
                MyEnumArray = await activitiesClient.ReturnEnumArray(new[] {input.Enum, input.Enum});

                return "Done";
            }
        }

        public class PrimitiveTypeOrchestrationInput
        {
            public byte Byte { get; set; }
            public sbyte SByte { get; set; }
            public int Integer { get; set; }
            public uint UInteger { get; set; }
            public short Short { get; set; }
            public ushort UShort { get; set; }
            public long Long { get; set; }
            public ulong ULong { get; set; }
            public float Float { get; set; }
            public double Double { get; set; }
            public char Char { get; set; }
            public bool Bool { get; set; }
            public string Str { get; set; }
            public decimal Decimal { get; set; }
            public DateTime DateTime { get; set; }
            public TimeSpan TimeSpan { get; set; }
            public TestEnum Enum { get; set; }
        }

        #endregion

        #region Adding more parameters to Activity and Orchestration input

        [TestMethod]
        public async Task MoreParamsOrchestrationTest()
        {
            await taskHub.AddTaskOrchestrations(typeof (MoreParamsOrchestration))
                .AddTaskActivitiesFromInterface<IMoreParamsActivities>(new MoreParamsActivities())
                .StartAsync();


            OrchestrationInstance id = await client.CreateOrchestrationInstanceAsync(typeof (MoreParamsOrchestration),
                new MoreParamsOrchestrationInput {Str = "Hello"});

            bool isCompleted = await TestHelpers.WaitForInstanceAsync(client, id, 60);
            Assert.IsTrue(isCompleted, TestHelpers.GetInstanceNotCompletedMessage(client, id, 60));

            Assert.AreEqual("Hello00Hello1015", MoreParamsOrchestration.Result);
        }

        public interface IMoreParamsActivities
        {
            string Activity1(string str, int i, double d);
            string Activity2(string str, int i = 10, double d = 15);
        }

        public interface IMoreParamsActivitiesClient
        {
            Task<string> Activity1(string str);
            Task<string> Activity2(string str);
        }

        public class MoreParamsActivities : IMoreParamsActivities
        {
            public string Activity1(string str, int i, double d)
            {
                return str + i + d;
            }

            public string Activity2(string str, int i = 10, double d = 15)
            {
                return str + i + d;
            }
        }

        sealed class MoreParamsOrchestration : TaskOrchestration<string, MoreParamsOrchestrationInput>
        {
            // HACK: This is just a hack to communicate result of orchestration back to test
            public static string Result;

            public override async Task<string> RunTask(OrchestrationContext context, MoreParamsOrchestrationInput input)
            {
                var activitiesClient = context.CreateClient<IMoreParamsActivitiesClient>();
                string a1 = await activitiesClient.Activity1(input.Str);
                string a2 = await activitiesClient.Activity2(input.Str);

                Result = a1 + a2;
                return Result;
            }
        }

        public class MoreParamsOrchestrationInput
        {
            public int Integer { get; set; }
            public double Double { get; set; }
            public string Str { get; set; }
        }

        #endregion
    }
}