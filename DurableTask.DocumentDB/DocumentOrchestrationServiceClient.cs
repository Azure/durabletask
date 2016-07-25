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

using System.Globalization;
using DurableTask.History;
using Microsoft.Azure.Documents;

namespace DurableTask.DocumentDb
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    public class DocumentOrchestrationServiceClient : IOrchestrationServiceClient
    {
        readonly SessionDocumentClient documentClient;

        public DocumentOrchestrationServiceClient(string documentDbEndpoint,
            string documentDbKey,
            string documentDbDatabase,
            string documentDbCollection)
        {
            this.documentClient = new SessionDocumentClient(
                documentDbEndpoint,
                documentDbKey,
                documentDbDatabase,
                documentDbCollection,
                true);
        }

        public async Task CreateTaskOrchestrationAsync(TaskMessage creationMessage)
        {
            if (!(creationMessage.Event is ExecutionStartedEvent))
            {
                // AFFANDAR : TODO : exception
                throw new Exception("Invalid creation message");
            }

            var orchestrationMessagesToAdd = new List<TaskMessageDocument>()
            {
                new TaskMessageDocument(creationMessage, Guid.NewGuid())
            };

            try
            {
                await
                    this.documentClient.UpdateSessionDocumentQueueAsync(
                        creationMessage.OrchestrationInstance.InstanceId,
                        null, 
                        orchestrationMessagesToAdd, 
                        null, 
                        null);
            }
            catch (DocumentClientException exception)
            {
                // AFFANDAR : TODO : exception
                throw new Exception("Error creating new orchestration: " + creationMessage.OrchestrationInstance.InstanceId, exception);
            }
        }

        public Task ForceTerminateTaskOrchestrationAsync(string instanceId, string reason)
        {
            throw new NotImplementedException();
        }

        public Task<string> GetOrchestrationHistoryAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }

        public Task<OrchestrationState> GetOrchestrationStateAsync(string instanceId, string executionId)
        {
            throw new NotImplementedException();
        }

        public Task<IList<OrchestrationState>> GetOrchestrationStateAsync(string instanceId, bool allExecutions)
        {
            throw new NotImplementedException();
        }

        public Task PurgeOrchestrationInstanceHistoryAsync(DateTime thresholdDateTimeUtc, OrchestrationStateTimeRangeFilterType timeRangeFilterType)
        {
            throw new NotImplementedException();
        }

        public async Task SendTaskOrchestrationMessageAsync(TaskMessage message)
        {
            var orchestrationMessagesToAdd = new List<TaskMessageDocument>()
            {
                new TaskMessageDocument(message, new Guid())
            };

            try
            {
                await this.documentClient.UpdateSessionDocumentQueueAsync(
                        message.OrchestrationInstance.InstanceId,
                        null, 
                        orchestrationMessagesToAdd, 
                        null, 
                        null);
            }
            catch (DocumentClientException exception)
            {
                // AFFANDAR : TODO : exception
                throw new Exception("Error creating new orchestration: " + message.OrchestrationInstance.InstanceId, exception);
            }
        }

        public Task<OrchestrationState> WaitForOrchestrationAsync(string instanceId, string executionId, TimeSpan timeout, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}
