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
                Common.TaskHubName,
                true);

            SessionDocument session = new SessionDocument { InstanceId = instanceId };

            session.SessionLock = new SessionLock { LockToken = lockToken, LockedUntilUtc = lockedUntilUtc };
            session.State = new OrchestrationState { Input = "foobar", Name = "testorch", Status = "running" };

            session.OrchestrationQueue = new List<TaskMessage>();
            session.OrchestrationQueue.Add(new TaskMessage { Event = new ExecutionStartedEvent(1, "foobar") });

            session.ActivityQueue = new List<TaskMessage>();
            session.ActivityQueue.Add(new TaskMessage { Event = new TaskScheduledEvent(2) });

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

            SessionDocument newResp = await c.UpdateSessionDocumentAsync(resp, false);

            Assert.IsNotNull(newResp);
            Assert.AreEqual(instanceId, newResp.InstanceId);
            Assert.IsNotNull(newResp.SessionLock);
            Assert.AreEqual(lockToken, newResp.SessionLock.LockToken);
            Assert.AreEqual(lockedUntilUtc, newResp.SessionLock.LockedUntilUtc);
            Assert.IsNotNull(newResp.State);
            Assert.AreEqual("newfoobar", newResp.State.Input);

            SessionDocument newResp2;

            // throw in a quick etag test while we are at it
            try
            {
                newResp2 = await c.UpdateSessionDocumentAsync(resp, true);
            }
            catch (Exception)
            {
                // AFFANDAR : TODO : validate actual exception
            }

            newResp2 = await c.UpdateSessionDocumentAsync(newResp, true);
            Assert.IsNotNull(newResp2);
            Assert.AreEqual(instanceId, newResp2.InstanceId);
            Assert.IsNull(newResp2.SessionLock);
            Assert.IsNotNull(newResp2.State);
            Assert.AreEqual("newfoobar", newResp2.State.Input);

            await c.DeleteSessionDocumentAsync(resp);

            resp = await c.FetchSessionDocumentAsync("instance123");

            Assert.IsNull(resp);
        }

        [TestMethod]
        public async Task BasicFetchAndCompleteTest()
        {
            // AFFANDAR : TODO : HERE HERE HERE.. add lock/complete/abandon tests
            DateTime lockedUntilUtc = DateTime.UtcNow.AddMinutes(5);

            SessionDocumentClient c = new SessionDocumentClient(
                Common.DocumentDbEndpoint,
                Common.DocumentDbKey,
                Common.DocumentDbDatabase,
                Common.TaskHubName,
                true);

            SessionDocument session = new SessionDocument { InstanceId = "lockable" };

            session.SessionLock = null;
            session.State = new OrchestrationState { Input = "foobar", Name = "testorch", Status = "running" };
            session.OrchestrationQueue = new List<TaskMessage>();
            session.ActivityQueue = null;
            session.OrchestrationQueue.Add(new TaskMessage { Event = new ExecutionStartedEvent(1, "foobar") });
            session.OrchestrationQueueLastUpdatedTimeUtc = DateTime.UtcNow;
            session.ActivityQueueLastUpdatedTimeUtc = DateTime.UtcNow;

            await c.CreateSessionDocumentAsync(session);

            var lockedDoc1 = await c.LockSessionDocumentAsync(true);
            var lockedDoc2 = await c.LockSessionDocumentAsync(true);

            Assert.IsNotNull(lockedDoc1);
            Assert.IsNull(lockedDoc2);

            await c.UpdateSessionDocumentAsync(lockedDoc1, true);

            lockedDoc2 = await c.LockSessionDocumentAsync(true);

            Assert.IsNotNull(lockedDoc2);

            await c.UpdateSessionDocumentAsync(lockedDoc2, true);

            try
            {
                await c.UpdateSessionDocumentAsync(lockedDoc2, true);
                Assert.Fail("should throw exception");
            }
            catch(Exception exception)
            {
            }

            var lockedDoc3 = await c.LockSessionDocumentAsync(false);
            Assert.IsNull(lockedDoc3);
        }
    }
}
