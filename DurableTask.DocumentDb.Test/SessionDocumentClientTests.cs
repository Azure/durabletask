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
        public async Task BasicCrudTest()
        {
            string instanceId = "instance123";
            string lockToken = "lock123";
            DateTime lockedUntilUtc = DateTime.UtcNow.AddMinutes(5);

            SessionDocumentClient c = new SessionDocumentClient(Common.DocumentDbEndpoint, Common.DocumentDbKey, Common.DocumentDbDatabase, Common.TaskHubName);

            SessionDocument session = new SessionDocument { InstanceId = instanceId };

            session.SessionLock = new SessionLock { LockToken = lockToken, LockedUntilUtc = lockedUntilUtc };
            session.State = new OrchestrationState { Input = "foobar", Name = "testorch", Status = "running" };
            session.OrchestrationQueue.Add(new TaskMessage { Event = new ExecutionStartedEvent(1, "foobar") });
            session.ActivityQueue.Add(new TaskMessage { Event = new TaskScheduledEvent(2) });

            await c.PutSessionDocumentAsync(session);

            SessionDocument resp = await c.GetSessionDocumentAsync("instance123");

            Assert.IsNotNull(resp);
            Assert.AreEqual(instanceId, resp.InstanceId);
            Assert.IsNotNull(resp.SessionLock);
            Assert.AreEqual(lockToken, resp.SessionLock.LockToken);
            Assert.AreEqual(lockedUntilUtc, resp.SessionLock.LockedUntilUtc);
        }

        [TestMethod]
        public async Task BasicCrudTest2()
        {
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
            session.ActivityQueue = new List<TaskMessage>();
            session.OrchestrationQueue.Add(new TaskMessage { Event = new ExecutionStartedEvent(1, "foobar") });
            session.ActivityQueue.Add(new TaskMessage { Event = new TaskScheduledEvent(2) });
            session.OrchestrationQueueLastUpdatedTimeUtc = DateTime.UtcNow;
            session.ActivityQueueLastUpdatedTimeUtc = DateTime.UtcNow;

            await c.PutSessionDocumentAsync(session);

            //session.InstanceId = "instance213423";
            //session.SessionLock = null;
            //session.OrchestrationQueue = null;
            //session.ActivityQueue = null;
            //session.OrchestrationQueueLastUpdatedTimeUtc = DateTime.UtcNow;
            //session.ActivityQueueLastUpdatedTimeUtc = DateTime.UtcNow;

            //await c.PutSessionDocumentAsync(session);

            //SessionDocument resp = await c.GetSessionDocumentAsync("instance123");
        }
    }
}
