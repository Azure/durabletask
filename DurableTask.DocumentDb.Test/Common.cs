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

namespace DurableTask.DocumentDb.Test
{
    using System;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using DurableTask;
    using System.Configuration;

    public static class Common
    {
        public static string DocumentDbEndpoint;
        public static string DocumentDbKey;
        public static string DocumentDbDatabase;
        public static string TaskHubName;

        static Common()
        {
            DocumentDbEndpoint = GetTestSetting("DocumentDbEndpoint");
            if (string.IsNullOrEmpty(DocumentDbEndpoint))
            {
                throw new Exception("Missing DocumentDbEndpoint in config");
            }

            DocumentDbKey = GetTestSetting("DocumentDbKey");
            if (string.IsNullOrEmpty(DocumentDbKey))
            {
                throw new Exception("Missing DocumentDbKey in config");
            }

            DocumentDbDatabase = GetTestSetting("DocumentDbDatabase");
            if (string.IsNullOrEmpty(DocumentDbDatabase))
            {
                throw new Exception("Missing DocumentDbDatabase in config");
            }

            TaskHubName = GetTestSetting("TaskHubName");
            if (string.IsNullOrEmpty(TaskHubName))
            {
                throw new Exception("Missing TaskHubName in config");
            }
        }

        static string GetTestSetting(string name)
        {
            string value = Environment.GetEnvironmentVariable("DurableTaskTest" + name);
            if (string.IsNullOrEmpty(value))
            {
                value = ConfigurationManager.AppSettings.Get(name);
            }

            return value;
        }

        public static IOrchestrationService CreateOrchestrationService(bool createNew)
        {
            var service = new DocumentOrchestrationService(
                DocumentDbEndpoint, 
                DocumentDbKey, 
                DocumentDbDatabase,
                TaskHubName);

            // AFFANDAR : TODO : async
            if (createNew)
            {
                service.CreateAsync().Wait();
            }
            else
            {
                service.CreateIfNotExistsAsync().Wait();
            }

            return service;
        }

        public static IOrchestrationServiceClient CreateOrchestrationServiceClient()
        {
            var service = new DocumentOrchestrationServiceClient(
                DocumentDbEndpoint,
                DocumentDbKey,
                DocumentDbDatabase,
                TaskHubName);

            return service;
        }

        public static TaskHubWorker CreateTaskHubWorker(bool createNew)
        {
            return new TaskHubWorker(CreateOrchestrationService(createNew));
        }

        public static TaskHubClient CreateTaskHubClient()
        {
            return new TaskHubClient(CreateOrchestrationServiceClient());
        }
    }
}
