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

using System.Linq;

namespace DurableTask.DocumentDb
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.Collections;
    using Microsoft.Azure.Documents;

    // AFFANDAR : TODO : MASTER
    //  + test for new orchestration
    //  + logging everywhere
    //  + suborchestration support
    //  + continue as new support
    //  + implement timer support
    //  + implememnt taskhub CRUD methods in DocumentOrchestrationService
    //  + fix up constant values in DocumentOrchestrationService
    //  + move sproc names to consts
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
        readonly SessionDocumentClient documentClient;
        readonly ConcurrentDictionary<string, SessionDocument> sessionMap;

        public DocumentOrchestrationService(string documentDbEndpoint,
        string documentDbKey,
        string documentDbDatabase,
        string documentDbCollection)
        {
            this.documentClient = new SessionDocumentClient(
                documentDbEndpoint,
                documentDbKey,
                documentDbDatabase,
                documentDbCollection);

            this.sessionMap = new ConcurrentDictionary<string, SessionDocument>();
        }

        // ************************************************
        // Settings
        // ************************************************

        public int MaxConcurrentTaskActivityWorkItems => 1;

        public int MaxConcurrentTaskOrchestrationWorkItems => 1;

        public int GetDelayInSecondsAfterOnFetchException(Exception exception) => 10;

        public int GetDelayInSecondsAfterOnProcessException(Exception exception) => 10;

        public bool IsMaxMessageCountExceeded(int currentMessageCount, OrchestrationRuntimeState runtimeState) => false;

        // ************************************************
        // Task Hub CRUD & state machine methods
        // ************************************************

        public Task CreateAsync()
        {
            return this.CreateDatastoreInternal(true);
        }

        public Task CreateAsync(bool recreateInstanceStore)
        {
            // with docdb we do not have the option of dropping/creating the instance store separately
            return this.CreateAsync();
        }

        public Task CreateIfNotExistsAsync()
        {
            return this.CreateDatastoreInternal(false);
        }

        Task CreateDatastoreInternal(bool createNew)
        {
            try
            {
                return this.documentClient.InitializeDatastoreAsync(createNew);
            }
            catch (DocumentClientException exception)
            {
                // AFFANDAR : TODO : exception
                throw new Exception("Error creating datastore", exception);
            }
        }

        public Task DeleteAsync()
        {
            try
            {
                return this.documentClient.DropDatastoreAsync();
            }
            catch (DocumentClientException exception)
            {
                // AFFANDAR : TODO : exception
                throw new Exception("Error dropping datastore", exception);
            }
        }

        public Task DeleteAsync(bool deleteInstanceStore)
        {
            // with docdb we do not have the option of dropping/creating the instance store separately
            return this.DeleteAsync();
        }

        // there is no runtime component to the doc db provider
        // so all of these will be no-ops

        public Task StartAsync()
        {
            return Task.FromResult<object>(null);
        }

        public Task StopAsync()
        {
            return Task.FromResult<object>(null);
        }

        public Task StopAsync(bool isForced)
        {
            return Task.FromResult<object>(null);
        }

        // ************************************************
        // Task Orchestration methods
        // ************************************************

        public async Task<TaskOrchestrationWorkItem> LockNextTaskOrchestrationWorkItemAsync(
            TimeSpan receiveTimeout,
            CancellationToken cancellationToken)
        {
            TaskOrchestrationWorkItem workItem = new TaskOrchestrationWorkItem();

            Stopwatch stopwatch = new Stopwatch();

            SessionDocument document = null;

            while (stopwatch.Elapsed < receiveTimeout)
            {
                try
                {
                    document = await this.documentClient.LockSessionDocumentAsync(true);
                    if (document != null)
                    {
                        break;
                    }
                }
                catch (DocumentClientException exception)
                {
                    // AFFANDAR : TODO : proper exception contract
                    throw new Exception("Failed to fetch workitem", exception);
                }

                await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);
            }

            if (document == null)
            {
                return null;
            }

            if (!this.sessionMap.TryAdd(document.InstanceId, document))
            {
                // AFFANDAR : TODO : proper exception contract
                // we can only get here if there was a bug
                throw new Exception("Failed to add session document to map, possible corruption. Session Id: " +
                                    document.InstanceId);
            }

            // AFFANDAR : TODO : HERE HERE HERE.. 
            //                  hammer out the initial session doc creation flow
            //                  move on to task activity methods
            workItem.OrchestrationRuntimeState = document.OrchestrationRuntimeState;
            workItem.InstanceId = document.InstanceId;
            workItem.LockedUntilUtc = document.SessionLock.LockedUntilUtc;
            workItem.NewMessages = document.OrchestrationQueue.Select(item => item.TaskMessage).ToList();

            return workItem;
        }


        public async Task ReleaseTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            SessionDocument document = null;
            if (!this.sessionMap.TryRemove(workItem.InstanceId, out document))
            {
                // AFFANDAR : TODO : log
                return;
            }

            document.SessionLock = null;

            try
            {
                await this.documentClient.CompleteSessionDocumentAsync(document);
            }
            catch (DocumentClientException exception)
            {
                // AFFANDAR : TODO : exception
                throw new Exception("Unable to release session: " + document.InstanceId, exception);
            }
        }

        public async Task CompleteTaskOrchestrationWorkItemAsync(
            TaskOrchestrationWorkItem workItem, 
            OrchestrationRuntimeState newOrchestrationRuntimeState, 
            IList<TaskMessage> outboundMessages, 
            IList<TaskMessage> orchestratorMessages, 
            IList<TaskMessage> timerMessages, 
            TaskMessage continuedAsNewMessage, 
            OrchestrationState orchestrationState)
        {
            if (continuedAsNewMessage != null)
            {
                // AFFANDAR : TODO : exception
                throw new NotSupportedException("ContinuedAsNew is not supported");
            }

            if (orchestratorMessages != null)
            {
                // AFFANDAR : TODO : exception
                throw new NotSupportedException("Suborchestrations are not supported");
            }

            if (timerMessages != null)
            {
                // AFFANDAR : TODO : exception
                throw new NotSupportedException("Timers are not supported");
            }

            SessionDocument document = null;
            if (!this.sessionMap.TryRemove(workItem.InstanceId, out document))
            {
                // AFFANDAR : TODO : exception
                throw new Exception("Unable to find session in session map: " + document.InstanceId);
            }

            document.OrchestrationRuntimeState = newOrchestrationRuntimeState;
            document.State = orchestrationState;
            document.SessionLock = null;

            IList<TaskMessageDocument> newActivityMessages = outboundMessages.Select(
                tm => new TaskMessageDocument
                {
                    MessageId = Guid.NewGuid(),
                    TaskMessage = tm
                }).ToList();

            try
            {
                await this.documentClient.CompleteSessionDocumentAsync(
                    document, 
                    document.OrchestrationQueue, 
                    null, 
                    null, 
                    newActivityMessages);
            }
            catch (DocumentClientException exception)
            {
                throw new Exception("Unable to release session: " + document.InstanceId, exception);
            }
        }

        public async Task AbandonTaskOrchestrationWorkItemAsync(TaskOrchestrationWorkItem workItem)
        {
            SessionDocument document = null;
            if (!this.sessionMap.TryRemove(workItem.InstanceId, out document))
            {
                // AFFANDAR : TODO : exception
                throw new Exception("Unable to find session in session map: " + document.InstanceId);
            }

            document.SessionLock = null;

            try
            {
                await this.documentClient.CompleteSessionDocumentAsync(document);
            }
            catch (DocumentClientException exception)
            {
                throw new Exception("Unable to release session: " + document.InstanceId, exception);
            }
        }

        // ************************************************
        // Task Activity methods
        // ************************************************

        public Task AbandonTaskActivityWorkItemAsync(TaskActivityWorkItem workItem)
        {
            throw new NotImplementedException();
        }

        public Task CompleteTaskActivityWorkItemAsync(TaskActivityWorkItem workItem, TaskMessage responseMessage)
        {
            throw new NotImplementedException();
        }

        public Task<TaskActivityWorkItem> LockNextTaskActivityWorkItem(TimeSpan receiveTimeout, CancellationToken cancellationToken)
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
    }
}
