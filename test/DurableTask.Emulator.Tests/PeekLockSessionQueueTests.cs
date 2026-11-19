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

namespace DurableTask.Emulator.Tests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.Core;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class PeekLockSessionQueueTests
    {
        [TestMethod]
        public async Task AcceptSessionKeepsLateMessagesForNextBatch()
        {
            var queue = new PeekLockSessionQueue();
            TaskMessage first = CreateMessage(1);
            TaskMessage late = CreateMessage(2);
            queue.SendMessage(first);

            TaskSession accepted = await queue.AcceptSessionAsync(TimeSpan.FromSeconds(1), CancellationToken.None);

            // Reproduce a producer appending after acceptance, before the consumer reads the batch.
            // This must not change the delivered batch or add messages absent from its lock table.
            queue.SendMessage(late);
            CollectionAssert.AreEqual(new[] { first }, accepted.Messages);

            byte[] state = { 1, 2, 3 };
            queue.CompleteSession(accepted.Id, state, Array.Empty<TaskMessage>(), null);

            TaskSession next = await queue.AcceptSessionAsync(TimeSpan.FromSeconds(1), CancellationToken.None);
            CollectionAssert.AreEqual(new[] { late }, next.Messages);
            CollectionAssert.AreEqual(state, next.SessionState);

            queue.CompleteSession(next.Id, state, Array.Empty<TaskMessage>(), null);
            Assert.IsNull(await queue.AcceptSessionAsync(TimeSpan.FromMilliseconds(1), CancellationToken.None));
        }

        [TestMethod]
        public async Task AbandonSessionRedeliversAcceptedAndLateMessages()
        {
            var queue = new PeekLockSessionQueue();
            TaskMessage first = CreateMessage(1);
            TaskMessage late = CreateMessage(2);
            queue.SendMessage(first);

            TaskSession accepted = await queue.AcceptSessionAsync(TimeSpan.FromSeconds(1), CancellationToken.None);
            queue.SendMessage(late);
            queue.AbandonSession(accepted.Id);

            TaskSession next = await queue.AcceptSessionAsync(TimeSpan.FromSeconds(1), CancellationToken.None);
            CollectionAssert.AreEqual(new[] { first, late }, next.Messages);
            CollectionAssert.AreEqual(new[] { first }, accepted.Messages);

            byte[] state = { 1 };
            queue.CompleteSession(next.Id, state, Array.Empty<TaskMessage>(), null);
            Assert.IsNull(await queue.AcceptSessionAsync(TimeSpan.FromMilliseconds(1), CancellationToken.None));
        }

        static TaskMessage CreateMessage(int eventId)
        {
            return new TaskMessage
            {
                OrchestrationInstance = new OrchestrationInstance { InstanceId = "instance", ExecutionId = "execution" },
                Event = new EventRaisedEvent(eventId, "payload") { Name = "event" },
            };
        }
    }
}
