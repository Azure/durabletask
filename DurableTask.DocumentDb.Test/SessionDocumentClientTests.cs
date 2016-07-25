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
    using System.Threading.Tasks;
    using History;
    using System.Collections.Generic;

    [TestClass]
    public class SessionDocumentClientTests
    {
        [TestMethod]
        public async Task BasicSessionDocCrudTest()
        {
            string instanceId = "instance123";
            string lockToken = "lock123";
            DateTime lockedUntilUtc = DateTime.UtcNow.AddMinutes(5);

            SessionDocumentClient c = new SessionDocumentClient(
                Common.DocumentDbEndpoint,
                Common.DocumentDbKey,
                Common.DocumentDbDatabase,
                Common.TaskHubName);

            await c.InitializeDatastoreAsync(true);

            SessionDocument session = new SessionDocument {InstanceId = instanceId};

            session.SessionLock = new SessionLock {LockToken = lockToken, LockedUntilUtc = lockedUntilUtc};
            session.State = new OrchestrationState {Input = "foobar", Name = "testorch", Status = "running"};

            session.OrchestrationQueue = new List<TaskMessageDocument>();
            session.OrchestrationQueue.Add(new TaskMessageDocument
            {
                MessageId = Guid.NewGuid(),
                TaskMessage = new TaskMessage {Event = new ExecutionStartedEvent(1, "foobar")}
            });

            session.ActivityQueue = new List<TaskMessageDocument>();
            session.ActivityQueue.Add(new TaskMessageDocument
            {
                MessageId = Guid.NewGuid(),
                TaskMessage = new TaskMessage {Event = new TaskScheduledEvent(2)}
            });

            await c.CreateSessionDocumentAsync(session);

            SessionDocument resp = await c.FetchSessionDocumentAsync("instance123");

            Assert.IsNotNull(resp);
            Assert.AreEqual(instanceId, resp.InstanceId);
            Assert.IsNotNull(resp.SessionLock);
            Assert.AreEqual(lockToken, resp.SessionLock.LockToken);
            Assert.AreEqual(lockedUntilUtc, resp.SessionLock.LockedUntilUtc);
            Assert.IsNotNull(resp.State);
            Assert.AreEqual("foobar", resp.State.Input);

            resp.State = new OrchestrationState {Input = "newfoobar"};

            SessionDocument newResp = await c.CompleteSessionDocumentAsync(resp);

            Assert.IsNotNull(newResp);
            Assert.AreEqual(instanceId, newResp.InstanceId);
            Assert.IsNull(newResp.SessionLock);
            Assert.IsNotNull(newResp.State);
            Assert.AreEqual("newfoobar", newResp.State.Input);

            SessionDocument newResp2;

            try
            {
                newResp2 = await c.CompleteSessionDocumentAsync(newResp);
                Assert.Fail("expected exception");
            }
            catch (Exception)
            {
            }

            await c.DeleteSessionDocumentAsync(resp);

            resp = await c.FetchSessionDocumentAsync("instance123");

            Assert.IsNull(resp);
        }

        /// <summary>
        /// + test multiple docs not lockable
        /// + test orchestrator lockable
        /// + test activity lockable
        /// + test both lockable
        /// </summary>
        /// <returns></returns>
        [TestMethod]
        public async Task OrchestrationLockAndCompleteTest()
        {
            DateTime lockedUntilUtc = DateTime.UtcNow.AddMinutes(5);

            SessionDocumentClient c = new SessionDocumentClient(
                Common.DocumentDbEndpoint,
                Common.DocumentDbKey,
                Common.DocumentDbDatabase,
                Common.TaskHubName);

            await c.InitializeDatastoreAsync(true);

            SessionDocument session = new SessionDocument {InstanceId = "lockable"};

            session.SessionLock = null;
            session.State = new OrchestrationState {Input = "foobar", Name = "testorch", Status = "running"};

            session.OrchestrationQueue = new List<TaskMessageDocument>();
            session.OrchestrationQueue.Add(new TaskMessageDocument
            {
                MessageId = Guid.NewGuid(),
                TaskMessage = new TaskMessage {Event = new ExecutionStartedEvent(1, "foobar")}
            });

            session.OrchestrationQueue.Add(new TaskMessageDocument
            {
                MessageId = Guid.NewGuid(),
                TaskMessage = new TaskMessage {Event = new ExecutionStartedEvent(1, "foobar2")}
            });

            session.OrchestrationQueueLastUpdatedTimeUtc = DateTime.UtcNow;

            SessionDocument session2 = new SessionDocument {InstanceId = "unlockable"};

            session2.SessionLock = null;
            session2.State = new OrchestrationState {Input = "foobar", Name = "testorch", Status = "running"};

            await c.CreateSessionDocumentAsync(session);
            await c.CreateSessionDocumentAsync(session2);

            // expect only one locked document
            var lockedDoc0 = await c.LockSessionDocumentAsync(false);
            var lockedDoc1 = await c.LockSessionDocumentAsync(true);
            var lockedDoc2 = await c.LockSessionDocumentAsync(true);

            Assert.IsNull(lockedDoc0);
            Assert.IsNotNull(lockedDoc1);
            Assert.IsNull(lockedDoc2);
            Assert.IsNotNull(lockedDoc1.SessionLock);

            // complete
            lockedDoc1.State.Input = "foobar2";

            await c.CompleteSessionDocumentAsync(lockedDoc1);

            // verify that the doc is now unlocked and was indeed updated
            var fetchedLockedDoc1 = await c.FetchSessionDocumentAsync(lockedDoc1.InstanceId);
            Assert.AreEqual("foobar2", fetchedLockedDoc1.State.Input);
            Assert.IsNull(fetchedLockedDoc1.SessionLock);

            // lock it again
            lockedDoc2 = await c.LockSessionDocumentAsync(true);

            Assert.IsNotNull(lockedDoc2);

            await c.CompleteSessionDocumentAsync(lockedDoc2);

            try
            {
                await c.CompleteSessionDocumentAsync(lockedDoc2);
                Assert.Fail("should throw exception");
            }
            catch (Exception exception)
            {
            }

            var lockedDoc3 = await c.LockSessionDocumentAsync(false);
            Assert.IsNull(lockedDoc3);
        }

        [TestMethod]
        public async Task ActivityLockAndCompleteTest()
        {
            SessionDocumentClient c = new SessionDocumentClient(
                Common.DocumentDbEndpoint,
                Common.DocumentDbKey,
                Common.DocumentDbDatabase,
                Common.TaskHubName);

            await c.InitializeDatastoreAsync(true);

            SessionDocument session = new SessionDocument {InstanceId = "lockable"};

            session.SessionLock = null;
            session.State = new OrchestrationState {Input = "foobar", Name = "testorch", Status = "running"};

            session.ActivityQueue = new List<TaskMessageDocument>();
            session.ActivityQueue.Add(new TaskMessageDocument
            {
                MessageId = Guid.NewGuid(),
                TaskMessage = new TaskMessage {Event = new ExecutionStartedEvent(1, "foobar")}
            });

            session.ActivityQueueLastUpdatedTimeUtc = DateTime.UtcNow;

            SessionDocument session2 = new SessionDocument {InstanceId = "unlockable"};

            session2.SessionLock = null;
            session2.State = new OrchestrationState {Input = "foobar", Name = "testorch", Status = "running"};

            await c.CreateSessionDocumentAsync(session);
            await c.CreateSessionDocumentAsync(session2);

            // expect only one locked document
            var lockedDoc0 = await c.LockSessionDocumentAsync(true);
            var lockedDoc1 = await c.LockSessionDocumentAsync(false);
            var lockedDoc2 = await c.LockSessionDocumentAsync(false);

            Assert.IsNull(lockedDoc0);
            Assert.IsNotNull(lockedDoc1);
            Assert.IsNull(lockedDoc2);
            Assert.IsNotNull(lockedDoc1.SessionLock);

            // complete
            lockedDoc1.State.Input = "foobar2";

            await c.CompleteSessionDocumentAsync(lockedDoc1);

            // verify that the doc is now unlocked and was indeed updated
            var fetchedLockedDoc1 = await c.FetchSessionDocumentAsync(lockedDoc1.InstanceId);
            Assert.AreEqual("foobar2", fetchedLockedDoc1.State.Input);
            Assert.IsNull(fetchedLockedDoc1.SessionLock);

            // lock it again
            lockedDoc2 = await c.LockSessionDocumentAsync(false);

            Assert.IsNotNull(lockedDoc2);

            await c.CompleteSessionDocumentAsync(lockedDoc2);

            try
            {
                await c.CompleteSessionDocumentAsync(lockedDoc2);
                Assert.Fail("should throw exception");
            }
            catch (Exception exception)
            {
            }

            var lockedDoc3 = await c.LockSessionDocumentAsync(true);
            Assert.IsNull(lockedDoc3);
        }

        [TestMethod]
        public async Task SessionQueueMessageCrudTest()
        {
            SessionDocumentClient c = new SessionDocumentClient(
                Common.DocumentDbEndpoint,
                Common.DocumentDbKey,
                Common.DocumentDbDatabase,
                Common.TaskHubName);

            await c.InitializeDatastoreAsync(true);

            SessionDocument session = new SessionDocument {InstanceId = "doc0"};

            session.SessionLock = null;
            session.State = new OrchestrationState {Input = "foobar", Name = "testorch", Status = "running"};

            SessionDocument session2 = new SessionDocument {InstanceId = "doc1"};

            session2.SessionLock = null;
            session2.State = new OrchestrationState {Input = "foobar", Name = "testorch", Status = "running"};

            await c.CreateSessionDocumentAsync(session);
            await c.CreateSessionDocumentAsync(session2);

            // expect only one locked document
            var lockedDoc0 = await c.LockSessionDocumentAsync(true);
            var lockedDoc1 = await c.LockSessionDocumentAsync(false);

            Assert.IsNull(lockedDoc0);
            Assert.IsNull(lockedDoc1);

            var fetchedDoc0 = await c.FetchSessionDocumentAsync("doc0");
            var fetchedDoc1 = await c.FetchSessionDocumentAsync("doc1");

            Assert.IsNotNull(fetchedDoc0);
            Assert.IsNotNull(fetchedDoc1);

            // insert some stuff into the queue

            TaskMessageDocument newOrchestrationMessage = new TaskMessageDocument
            {
                MessageId = Guid.NewGuid(),
                TaskMessage = new TaskMessage {Event = new ExecutionStartedEvent(1, "newinput")}
            };

            TaskMessageDocument newActivityMessage = new TaskMessageDocument
            {
                MessageId = Guid.NewGuid(),
                TaskMessage = new TaskMessage { Event = new TaskScheduledEvent(1) }
            };

            List<TaskMessageDocument> orchToAdd = new List<TaskMessageDocument> {newOrchestrationMessage};
            List<TaskMessageDocument> activityToAdd = new List<TaskMessageDocument> { newActivityMessage };

            var updatedDoc0 = await c.UpdateSessionDocumentQueueAsync("doc0", null, orchToAdd, null, activityToAdd);

            Assert.IsNotNull(updatedDoc0.ActivityQueue);
            Assert.IsNotNull(updatedDoc0.OrchestrationQueue);

            // remove all the orchestration messages
            updatedDoc0 = await c.UpdateSessionDocumentQueueAsync("doc0", orchToAdd, null, null, null);

            Assert.IsNotNull(updatedDoc0.ActivityQueue);
            Assert.IsNull(updatedDoc0.OrchestrationQueue);

            // remove all the activity messages
            updatedDoc0 = await c.UpdateSessionDocumentQueueAsync("doc0", null, null, activityToAdd, null);

            Assert.IsNull(updatedDoc0.ActivityQueue);
            Assert.IsNull(updatedDoc0.OrchestrationQueue);
        }
    }
}
