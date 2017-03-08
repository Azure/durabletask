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

using System;
using System.Collections.Immutable;
using DurableTask.History;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DurableTask.ServiceFabric.UnitTests
{
    [TestClass]
    public class TimerFiredEventComparerTests
    {
        [TestMethod]
        public void Messages_With_Equal_FiredAt_Value_With_Different_Key_Are_Treated_Separate_And_Sorted_On_Key()
        {
            ImmutableSortedSet<Message<string, TaskMessage>> timerMessages = ImmutableSortedSet<Message<string, TaskMessage>>.Empty.WithComparer(TimerFiredEventComparer.Instance);

            var instance = new OrchestrationInstance()
            {
                InstanceId = "InstanceId",
            };

            var currentTime = DateTime.UtcNow;
            var builder = timerMessages.ToBuilder();
            int numberOfMessages = 5;

            // Add them in reverse order to expected order just to make the test case more interesting.
            for (int i = numberOfMessages; i >= 1; i--)
            {
                builder.Add(new Message<string, TaskMessage>($"Message{i}", new TaskMessage()
                {
                    SequenceNumber = i,
                    OrchestrationInstance = instance,
                    Event = new TimerFiredEvent(i) { FireAt = currentTime }
                }));
            }

            Assert.AreEqual(numberOfMessages, builder.Count);
            timerMessages = builder.ToImmutableSortedSet(TimerFiredEventComparer.Instance);
            Assert.AreEqual(numberOfMessages, timerMessages.Count);

            // Now enumerating the items should result in right order.
            for (int i = 1; i <= numberOfMessages; i++)
            {
                var min = timerMessages.Min;
                timerMessages = timerMessages.Remove(min);
                Assert.AreEqual($"Message{i}", min.Key, "Ordering seems to be broken");
            }

            Assert.AreEqual(0, timerMessages.Count);
        }

        [TestMethod]
        public void Messages_With_Different_FiredAt_Value_Are_Sorted_Based_On_FiredAt()
        {
            ImmutableSortedSet<Message<string, TaskMessage>> timerMessages = ImmutableSortedSet<Message<string, TaskMessage>>.Empty.WithComparer(TimerFiredEventComparer.Instance);

            var instance = new OrchestrationInstance()
            {
                InstanceId = "InstanceId",
            };

            var currentTime = DateTime.UtcNow;
            var builder = timerMessages.ToBuilder();
            int numberOfMessages = 5;

            // Add them in reverse order to expected order just to make the test case more interesting.
            for (int i = numberOfMessages; i >= 1; i--)
            {
                builder.Add(new Message<string, TaskMessage>($"Message{6-i}", new TaskMessage() //Let the key values be in reverse order to fired at values again to make test more interesting.
                {
                    SequenceNumber = i,
                    OrchestrationInstance = instance,
                    Event = new TimerFiredEvent(i) { FireAt = currentTime + TimeSpan.FromSeconds(i) }
                }));
            }

            Assert.AreEqual(numberOfMessages, builder.Count);
            timerMessages = builder.ToImmutableSortedSet(TimerFiredEventComparer.Instance);
            Assert.AreEqual(numberOfMessages, timerMessages.Count);

            // Now enumerating the items should result in right order.
            for (int i = 1; i <= numberOfMessages; i++)
            {
                var min = timerMessages.Min;
                timerMessages = timerMessages.Remove(min);
                var firedAt = (min.Value.Event as TimerFiredEvent)?.FireAt;
                Assert.AreEqual(currentTime + TimeSpan.FromSeconds(i), firedAt, "Ordering seems to be broken");
                Assert.AreEqual($"Message{6 - i}", min.Key);
            }

            Assert.AreEqual(0, timerMessages.Count);
        }

        [TestMethod]
        public void Messages_With_Equal_FiredAt_Value_With_Same_Key_Are_Treated_Equal()
        {
            ImmutableSortedSet<Message<string, TaskMessage>> timerMessages = ImmutableSortedSet<Message<string, TaskMessage>>.Empty.WithComparer(TimerFiredEventComparer.Instance);

            var instance = new OrchestrationInstance()
            {
                InstanceId = "InstanceId",
            };

            var currentTime = DateTime.UtcNow;
            var builder = timerMessages.ToBuilder();
            int numberOfMessages = 3;

            // Add them in reverse order to expected order just to make the test case more interesting.
            for (int i = numberOfMessages; i >= 1; i--)
            {
                builder.Add(new Message<string, TaskMessage>($"Message", new TaskMessage()
                {
                    SequenceNumber = i,
                    OrchestrationInstance = instance,
                    Event = new TimerFiredEvent(i) { FireAt = currentTime }
                }));
            }

            Assert.AreEqual(1, builder.Count);
            timerMessages = builder.ToImmutableSortedSet(TimerFiredEventComparer.Instance);
            Assert.AreEqual(1, timerMessages.Count);
        }
    }
}
