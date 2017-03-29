
using DurableTask;

namespace FrameworkUnitTests
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class UtilsTests
    {
        public const string StubString = "StubString";

        [TestMethod]
        public void TruncateShouldReturnInputIfEmptyStringIsUsed()
        {
            var result = string.Empty.Truncate(15);

            Assert.AreEqual(result, string.Empty);
        }

        [TestMethod]
        public void TruncateShouldReturnInputIfLengthGreaterThanMaxLenth()
        {
            var result = StubString.Truncate(15);

            Assert.AreEqual(result, StubString);
        }

        [TestMethod]
        public void TruncateShouldReturnTheCorrectSubString()
        {
            var result = StubString.Truncate(5);

            Assert.AreEqual(result, "StubS");
        }

        [TestMethod]
        public void EscapeJsonTest()
        {
            var jsonString = "{\"environ=;ment\": \"PROD\"}";

            var result = Utils.EscapeJson(jsonString);

            Assert.AreEqual(result, "{{\"environ%3D%3Bment\": \"PROD\"}}");
        }
    }
}