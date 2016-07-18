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

namespace DurableTask.DocumentDb
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    // AFFANDAR : TODO : MASTER
    //  + add continuation token to sps: https://github.com/Azure/azure-documentdb-js-server/blob/master/samples/stored-procedures/update.js
    //  + move all doc db calls to stored procs for consistency?
    //  + add partitioning
    //  + DAL tests
    //  + actually hook up DAL to orchestration service
    //  + data driven stored proc script provisioning
    //  + write doc db DAL for OM
    //  + implement orchestration service
    //  + impl orch svc client
    //  + impl orch svs instances store.. is this really needed with doc db?
    //  
    //  DONE:
    //  + write data model classes for session + queue-let
    //
    //  POSTPONED:
    //  + separate out queues from master doc
    //      + sproc for inserting into a queue
    //      + sproc for deleting upto a 
    //  
    public class DocumentOrchestrationService : IOrchestrationService
    {
        public int MaxConcurrentTaskActivityWorkItems
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public int MaxConcurrentTaskOrchestrationWorkItems
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
        {
            throw new NotImplementedException();
        }

        public Task CompleteTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem, OrchestrationRuntimeState newOrchestrationRuntimeState, IList<TaskMessage> outboundMessages, IList<TaskMessage> orchestratorMessages, IList<TaskMessage> timerMessages, TaskMessage continuedAsNewMessage, OrchestrationState orchestrationState)
        {
            throw new NotImplementedException();
        }

        public Task CreateAsync()
        {
            throw new NotImplementedException();
        }

        public Task CreateAsync(bool recreateInstanceStore)
        {
            throw new NotImplementedException();
        }

        public Task CreateIfNotExistsAsync()
        {
            throw new NotImplementedException();
        }

        public Task DeleteAsync()
        {
            throw new NotImplementedException();
        }

        public Task DeleteAsync(bool deleteInstanceStore)
        {
            throw new NotImplementedException();
        }

        public int GetDelayInSecondsAfterOnFetchException(Exception exception)
        {
            throw new NotImplementedException();
        }

        public int GetDelayInSecondsAfterOnProcessException(Exception exception)
        {
            throw new NotImplementedException();
        }

        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState)
        {
            throw new NotImplementedException();
        }

        public Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(TimeSpan receiveTimeout, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public Task<TaskActivityWorkItem> RenewTaskActivityWorkItemLockAsync(TaskActivityWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public Task RenewTaskOrchestrationWorkItemLockAsync(TaskOrchestrationWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public Task StartAsync()
        {
            throw new NotImplementedException();
        }

        public Task StopAsync()
        {
            throw new NotImplementedException();
        }

        public Task StopAsync(bool isForced)
        {
            throw new NotImplementedException();
        }
    }
}
