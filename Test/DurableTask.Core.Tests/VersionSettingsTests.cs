using DurableTask.Core.Settings;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DurableTask.Core.Tests
{
    [TestClass]
    public class VersionSettingsTests
    {
        [TestMethod]
        [DataRow("1.0.0", "1.0.0", 0)]
        [DataRow("1.1.0", "1.0.0", 1)]
        [DataRow("1.0.0", "1.1.0", -1)]
        [DataRow("1", "1", 0)]
        [DataRow("2", "1", 1)]
        [DataRow("1", "2", -1)]
        [DataRow("", "1", -1)]
        [DataRow("1", "", 1)]
        [DataRow("", "", 0)]
        public void TestVersionComparison(string orchVersion, string settingVersion, int expectedComparison)
        {
            int result = VersioningSettings.CompareVersions(orchVersion, settingVersion);

            if (expectedComparison == 0)
            {
                Assert.AreEqual(0, result, $"Expected {orchVersion} to be equal to {settingVersion}");
            }
            else if (expectedComparison < 0)
            {
                Assert.IsTrue(result < 0, $"Expected {orchVersion} to be less than {settingVersion}");
            }
            else
            {
                Assert.IsTrue(result > 0, $"Expected {orchVersion} to be greater than {settingVersion}");
            }
        }
    }
}