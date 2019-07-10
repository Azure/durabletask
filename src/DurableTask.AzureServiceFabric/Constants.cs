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

namespace DurableTask.AzureServiceFabric
{
    static class Constants
    {
        internal const string CollectionNameUniquenessPrefix = "DtfxSfp_";
        internal const string SessionMessagesDictionaryPrefix = CollectionNameUniquenessPrefix + "SessionMessages_";
        internal const string OrchestrationDictionaryName = CollectionNameUniquenessPrefix + "Orchestrations";
        internal const string ActivitiesQueueName = CollectionNameUniquenessPrefix + "Activities";
        internal const string InstanceStoreDictionaryName = CollectionNameUniquenessPrefix + "InstanceStore";
        internal const string ExecutionStoreDictionaryName = CollectionNameUniquenessPrefix + "ExecutionIdStore";
        internal const string ScheduledMessagesDictionaryName = CollectionNameUniquenessPrefix + "ScheduledMessages";
        internal const string TaskHubProxyServiceName = "DurableTask-TaskHubProxyService";
        internal const string TaskHubProxyListenerEndpointName = "DtfxServiceEndpoint";
    }
}
