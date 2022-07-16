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

namespace DurableTask.Redis
{
    /// <summary>
    /// Helper methods to make getting the corresponding Redis Key for the various Redis data
    /// structures easier.
    /// </summary>
    static class RedisKeyNameResolver
    {
        public static string GetPartitionControlQueueKey(string taskHubName, string partition)
            => $"{taskHubName}.{partition}.ControlQueue";

        public static string GetPartitionControlNotificationChannelKey(string taskHubName, string partition)
            => $"{taskHubName}.{partition}.ControlQueue.Notifications";

        public static string GetOrchestrationQueueKey(string taskHubName, string partition, string orchestrationId)
            => $"{taskHubName}.{partition}.ControlQueue.{orchestrationId}";

        public static string GetOrchestrationRuntimeStateHashKey(string taskHubName, string partition)
            => $"{taskHubName}.{partition}.OrchestrationsRuntimeState";

        public static string GetOrchestrationsSetKey(string taskHubName, string partition)
            => $"{taskHubName}.{partition}.Orchestrations";

        public static string GetOrchestrationStateKey(string taskHubName, string partition, string orchestrationId)
            => $"{taskHubName}.{partition}.OrchestrationsState.{orchestrationId}";

        public static string GetTaskActivityIncomingQueueKey(string taskHubName)
            => $"{taskHubName}.IncomingActivityMessages";

        public static string GetTaskActivityProcessingQueueKey(string taskHubName, string workerId)
            => $"{taskHubName}.ProcessingActivityMessages.{workerId}";

        public static string GetWorkerSetKey(string taskHubName)
            => $"{taskHubName}.Workers";

        public static string GetTraceLogsKey(string taskHubName)
            => $"{taskHubName}.TraceLogs";
    }
}
