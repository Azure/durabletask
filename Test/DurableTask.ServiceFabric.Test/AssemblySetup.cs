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

using System;
using System.IO;
using DurableTask.ServiceFabric.Test.DeploymentUtil;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DurableTask.ServiceFabric.Test
{
    [TestClass]
    public class AssemblySetup
    {
        [AssemblyInitialize]
        public static void AssemblyInitialize(TestContext testContext)
        {
            //Todo: This will fail if local cluster is not setup, currently the test code does not automatically
            //start a local cluster and that's a manual pre-req step.
            DeploymentHelper.CleanAsync().Wait();
            DeploymentHelper.DeployAsync(TestApplicationRootPath).Wait();
        }

        [AssemblyCleanup]
        public static void AssemblyCleanup()
        {
            DeploymentHelper.CleanAsync().Wait();
        }

        static string TestApplicationRootPath
        {
            get
            {
                var applicationPath = Path.Combine(Environment.GetEnvironmentVariable("SourceRoot") ?? string.Empty, "Test", "TestFabricApplication", "TestFabricApplication");

                if (!Directory.Exists(applicationPath))
                {
                    throw new Exception("Could not find test application path, define SourceRoot environment variable to the source path");
                }

                if (!Directory.Exists(Path.Combine(applicationPath, "pkg", "Debug")))
                {
                    throw new Exception("Could not find test application package, make sure the test application is built and package generated before running the tests");
                }

                return applicationPath;
            }
        }
    }
}
