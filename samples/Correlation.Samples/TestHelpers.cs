﻿//  ----------------------------------------------------------------------------------
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

namespace Correlation.Samples
{
    using System;
    using DurableTask.AzureStorage;
    using System.Configuration;
    using System.IO;
    using Microsoft.Extensions.Configuration;

    public static class TestHelpers
    {
        public static IConfigurationRoot Configuration { get; set; }

        static TestHelpers()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json");
            Configuration = builder.Build();
        }

        internal static TestOrchestrationHost GetTestOrchestrationHost(
            bool enableExtendedSessions,
            int extendedSessionTimeoutInSeconds = 30)
        {
            string storageConnectionString = GetTestStorageAccountConnectionString();

            var settings = new AzureStorageOrchestrationServiceSettings
            {
                StorageConnectionString = storageConnectionString,
                TaskHubName = Configuration["taskHubName"],
                ExtendedSessionsEnabled = enableExtendedSessions,
                ExtendedSessionIdleTimeout = TimeSpan.FromSeconds(extendedSessionTimeoutInSeconds),
            };

            return new TestOrchestrationHost(settings);
        }

        public static string GetTestStorageAccountConnectionString()
        {
            string storageConnectionString = GetTestSetting("StorageConnectionString");
            if (string.IsNullOrEmpty(storageConnectionString))
            {
                throw new ArgumentNullException("A Storage connection string must be defined in either an environment variable or in appsettings.json.");
            }

            return storageConnectionString;
        }

        static string GetTestSetting(string name)
        {
            string value = Environment.GetEnvironmentVariable("DurableTaskTest" + name);
            if (string.IsNullOrEmpty(value))
            {
                value = Configuration[name];
            }

            return value;
        }
    }
}
