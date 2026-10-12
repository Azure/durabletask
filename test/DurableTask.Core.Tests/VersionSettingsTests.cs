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

using DurableTask.Core.Settings;
using Microsoft.VisualStudio.TestTools.UnitTesting;

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