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

namespace DurableTask.Core.Tests
{
    using System.Diagnostics;
    using System.Reflection;
    using System.Reflection.Emit;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class AssemblyFileVersionTests
    {
        [TestMethod]
        public void GetAssemblyFileVersion_UsesFileVersionInfoWhenLocationIsKnown()
        {
            Assembly assembly = typeof(AssemblyFileVersionTests).Assembly;
            string expected = FileVersionInfo.GetVersionInfo(assembly.Location).FileVersion;

            Assert.AreEqual(expected, DurableTask.Core.Common.Utils.GetAssemblyFileVersion(assembly));
        }

#if !NETFRAMEWORK
        [TestMethod]
        public void GetAssemblyFileVersion_FallsBackToAttributeWhenLocationIsEmpty()
        {
            // A dynamic assembly has an empty Location, like an assembly inside a single-file bundle.
            var name = new AssemblyName("AssemblyFileVersionTestAssembly");
            AssemblyBuilder assembly = AssemblyBuilder.DefineDynamicAssembly(name, AssemblyBuilderAccess.Run);
            ConstructorInfo ctor = typeof(AssemblyFileVersionAttribute).GetConstructor(new[] { typeof(string) });
            assembly.SetCustomAttribute(new CustomAttributeBuilder(ctor, new object[] { "1.2.3.4" }));

            Assert.AreEqual(string.Empty, assembly.Location);
            Assert.AreEqual("1.2.3.4", DurableTask.Core.Common.Utils.GetAssemblyFileVersion(assembly));
        }
#endif
    }
}
