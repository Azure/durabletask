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

namespace DurableTask.Core.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Threading.Tasks;
    using DurableTask.Core.Command;
    using DurableTask.Core.History;
    using DurableTask.Core.Serializing;
    using DurableTask.Core.Settings;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Newtonsoft.Json;

    [TestClass]
    public class SubOrchestrationInstanceIdValidatorTests
    {
        [TestMethod]
        public void SameBatchDuplicateProducesActionableNonRetriableFailure()
        {
            CreateSubOrchestrationAction first = Child(3, "child-id");
            first.Name = "first-name";
            first.Version = "v1";
            first.Input = "private-first-input";
            CreateSubOrchestrationAction second = Child(7, "child-id");
            second.Name = "different-name";
            second.Version = "v2";
            second.Input = "private-second-input";

            OrchestrationCompleteOrchestratorAction failure = Validate(Array.Empty<HistoryEvent>(), first, second);

            Assert.IsNotNull(failure);
            Assert.AreEqual(7, failure.Id);
            Assert.AreEqual(OrchestrationStatus.Failed, failure.OrchestrationStatus);
            Assert.AreEqual("DuplicateSubOrchestrationInstanceId", failure.FailureDetails.ErrorType);
            Assert.IsTrue(failure.FailureDetails.IsNonRetriable);
            Assert.AreEqual(failure.Result, failure.FailureDetails.ErrorMessage);
            Assert.IsNull(failure.FailureDetails.InnerFailure);
            Assert.IsNull(failure.FailureDetails.StackTrace);
            StringAssert.Contains(failure.Result, "parent-id");
            StringAssert.Contains(failure.Result, "child-id");
            StringAssert.Contains(failure.Result, "task ID 3");
            StringAssert.Contains(failure.Result, "task ID 7");
            StringAssert.Contains(failure.Result, "distinct instance IDs");
            StringAssert.Contains(failure.Result, "automatically");
            StringAssert.Contains(failure.Result, "await completion");
            Assert.IsFalse(failure.Result.Contains("private-"));
        }

        [TestMethod]
        public void PendingChildFromPreviousEpisodeConflicts()
        {
            var history = new HistoryEvent[] { Created(2, "child-id"), new OrchestratorCompletedEvent(-1) };
            OrchestrationCompleteOrchestratorAction failure = Validate(history, Child(5, "child-id"));
            Assert.IsNotNull(failure);
            StringAssert.Contains(failure.Result, "task ID 2");
            StringAssert.Contains(failure.Result, "task ID 5");
        }

        [DataTestMethod]
        [DataRow(false, false)]
        [DataRow(true, false)]
        [DataRow(false, true)]
        [DataRow(true, true)]
        public void CompletedOrFailedChildCanBeReused(bool failed, bool completionIsNew)
        {
            HistoryEvent completion = Completion(2, failed);
            var runtimeState = new OrchestrationRuntimeState(new HistoryEvent[] { Created(2, "child-id") });
            if (completionIsNew)
            {
                runtimeState.AddEvent(completion);
                Assert.IsFalse(runtimeState.PastEvents.Contains(completion));
                Assert.IsTrue(runtimeState.NewEvents.Contains(completion));
            }
            else
            {
                runtimeState = new OrchestrationRuntimeState(new HistoryEvent[] { Created(2, "child-id"), completion });
            }

            Assert.IsNull(Validate(runtimeState.Events, Child(5, "child-id")));
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void DuplicateOldCompletionDoesNotRemoveNewerPendingChild(bool failed)
        {
            var history = new HistoryEvent[]
            {
                Created(2, "child-id"),
                Completion(2, failed),
                Created(5, "child-id"),
                Completion(2, failed),
            };
            OrchestrationCompleteOrchestratorAction failure = Validate(history, Child(8, "child-id"));
            Assert.IsNotNull(failure);
            StringAssert.Contains(failure.Result, "task ID 5");
        }

        [TestMethod]
        public void LegacyDuplicatesOnlyRejectNewConflictingStarts()
        {
            var history = new HistoryEvent[]
            {
                Created(0, "child-id"),
                Created(1, "child-id"),
                Completion(1, false),
            };
            Assert.IsNull(Validate(history));
            Assert.IsNull(Validate(history, new OrchestrationCompleteOrchestratorAction { OrchestrationStatus = OrchestrationStatus.Completed }));
            Assert.IsNull(Validate(history, Child(2, "different-id")));
            Assert.IsNotNull(Validate(history, Child(2, "child-id")));
        }

        [TestMethod]
        public void LegacyDuplicateIdsBecomeAvailableOnlyAfterAllTasksComplete()
        {
            var history = new List<HistoryEvent>
            {
                Created(0, "child-id"),
                Created(1, "child-id"),
                Completion(0, false),
            };
            Assert.IsNotNull(Validate(history, Child(2, "child-id")));
            history.Add(Completion(1, true));
            Assert.IsNull(Validate(history, Child(2, "child-id")));
        }

        [TestMethod]
        public void DistinctAndCaseSensitiveIdsAreAllowed()
        {
            Assert.IsNull(Validate(
                new[] { Created(0, "child-id") },
                Child(1, "CHILD-ID"),
                Child(2, "other-id")));
        }

        [TestMethod]
        public void AutomaticallyGeneratedIdsAreDistinct()
        {
            var context = new TaskOrchestrationContext(
                new OrchestrationInstance { InstanceId = "parent-id", ExecutionId = "execution-id" },
                TaskScheduler.Default);
            Task<string> first = context.CreateSubOrchestrationInstance<string>("child", "", null);
            Task<string> second = context.CreateSubOrchestrationInstance<string>("child", "", null);
            Assert.IsFalse(first.IsCompleted);
            Assert.IsFalse(second.IsCompleted);
            var actions = context.OrchestratorActions.Cast<CreateSubOrchestrationAction>().ToArray();
            Assert.AreEqual(2, actions.Length);
            Assert.AreNotEqual(actions[0].InstanceId, actions[1].InstanceId);
            Assert.IsNull(Validate(Array.Empty<HistoryEvent>(), actions));
        }

        [TestMethod]
        public void FireAndForgetStartsAreExcludedInHistoryAndDecisions()
        {
            var tags = new Dictionary<string, string> { { OrchestrationTags.FireAndForget, "" } };
            SubOrchestrationInstanceCreatedEvent previous = Created(0, "child-id");
            previous.Tags = tags;
            CreateSubOrchestrationAction detached = Child(1, "child-id");
            detached.Tags = tags;

            Assert.IsNull(Validate(new[] { previous }, Child(2, "child-id")));
            Assert.IsNull(Validate(new[] { Created(0, "child-id") }, detached));
            Assert.IsNull(Validate(Array.Empty<HistoryEvent>(), detached, Child(2, "child-id")));
            Assert.IsNull(Validate(Array.Empty<HistoryEvent>(), Child(2, "child-id"), detached));
            Assert.IsNull(Validate(Array.Empty<HistoryEvent>(), detached, detached));
            Assert.IsNotNull(Validate(Array.Empty<HistoryEvent>(), detached, Child(2, "child-id"), Child(3, "child-id")));
        }

        [TestMethod]
        public void NoAwaitedStartsDoesNotEnumerateHistory()
        {
            var tags = new CountingTags();
            var created = Created(0, "pending-child");
            created.Tags = tags;
            var state = new OrchestrationRuntimeState(new[] { created });
            CreateSubOrchestrationAction detached = Child(1, "child-id");
            detached.Tags = new Dictionary<string, string> { { OrchestrationTags.FireAndForget, "" } };
            Assert.IsNull(Validate(state));
            Assert.IsNull(Validate(state, detached, new CreateTimerOrchestratorAction { Id = 2 }));
            Assert.AreEqual(0, tags.Reads);
            Assert.IsNull(typeof(OrchestrationRuntimeState)
                .GetField("subOrchestrationInstanceIdIndex", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(state));
        }

        [TestMethod]
        public void RepeatedValidationDoesNotReadHistoricalChildTagsAgain()
        {
            var tags = new CountingTags();
            var created = Created(0, "pending-child");
            created.Tags = tags;
            var runtimeState = new OrchestrationRuntimeState(new[] { created });
            Assert.IsNull(Validate(runtimeState, Child(1, "other-child")));
            Assert.AreEqual(1, tags.Reads);
            Assert.IsNull(Validate(runtimeState, Child(1, "other-child")));
            Assert.AreEqual(1, tags.Reads, "An unchanged runtime state must not rescan historical child tags.");
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void AcceptedEventsUpdateAnInitializedIndex(bool failed)
        {
            var tags = new CountingTags();
            var created = Created(0, "child-id");
            created.Tags = tags;
            var state = new OrchestrationRuntimeState(new[] { created });
            Assert.IsNotNull(Validate(state, Child(1, "child-id")));
            state.AddEvent(Completion(0, failed));
            Assert.IsNull(Validate(state, Child(1, "child-id")));
            state.AddEvent(Created(1, "child-id"));
            state.AddEvent(Completion(0, failed));
            OrchestrationCompleteOrchestratorAction failure = Validate(state, Child(2, "child-id"));
            Assert.IsNotNull(failure);
            StringAssert.Contains(failure.Result, "task ID 1");
            Assert.AreEqual(1, tags.Reads, "Incremental updates must not rebuild prior history.");
        }

        [DataTestMethod]
        [DataRow(0, 1, 2)]
        [DataRow(1, 2, 0)]
        [DataRow(2, 0, 1)]
        public void LegacyDuplicateCompletionsRemoveOnlyTheirMatchingTask(int first, int second, int last)
        {
            var state = new OrchestrationRuntimeState(new[]
            {
                Created(0, "child-id"), Created(1, "child-id"), Created(2, "child-id"),
            });
            Assert.IsNotNull(Validate(state, Child(3, "child-id")));
            state.AddEvent(Completion(first, false));
            Assert.IsNotNull(Validate(state, Child(3, "child-id")));
            state.AddEvent(Completion(second, true));
            state.AddEvent(Completion(first, false));
            OrchestrationCompleteOrchestratorAction failure = Validate(state, Child(3, "child-id"));
            Assert.IsNotNull(failure);
            StringAssert.Contains(failure.Result, $"task ID {last}");
            state.AddEvent(Completion(last, false));
            Assert.IsNull(Validate(state, Child(3, "child-id")));
        }

        [TestMethod]
        public void ProposedAndRejectedActionsNeverBecomeAcceptedHistory()
        {
            var state = new OrchestrationRuntimeState();
            Assert.IsNull(Validate(state, Child(0, "first"), Child(1, "second")));
            Assert.IsNull(Validate(state, Child(0, "first"), Child(1, "second")));
            Assert.AreEqual(0, state.Events.Count);
            state.AddEvent(Created(0, "first"));
            Assert.IsNull(Validate(state, Child(1, "second")));
            Assert.IsNotNull(Validate(state, Child(1, "second"), Child(2, "second")));
            Assert.IsNull(Validate(state, Child(1, "second")));
            Assert.AreEqual(1, state.Events.Count, "Unsent actions from a split or rejected batch must not poison the index.");
        }

        [TestMethod]
        public void ReloadedHistoryDoesNotReuseThePreviousRuntimeIndex()
        {
            var state = new OrchestrationRuntimeState(new[] { Created(0, "child-id") });
            Assert.IsNotNull(Validate(state, Child(1, "child-id")));
            var restored = new OrchestrationRuntimeState(state.Events);
            state.AddEvent(Completion(0, false));
            Assert.IsNull(Validate(state, Child(1, "child-id")));
            Assert.IsNotNull(Validate(restored, Child(1, "child-id")));
            restored.AddEvent(Completion(0, true));
            Assert.IsNull(Validate(restored, Child(1, "child-id")));
        }

        [TestMethod]
        public void SameCountHistoryReplacementIsRebuiltAfterInvalidation()
        {
            var state = new OrchestrationRuntimeState(new[] { Created(0, "old-id") });
            Assert.IsNotNull(Validate(state, Child(1, "old-id")));
            state.Events[0] = Created(0, "new-id");
            state.InvalidateSubOrchestrationInstanceIdIndex();
            Assert.IsNull(Validate(state, Child(1, "old-id")));
            Assert.IsNotNull(Validate(state, Child(1, "new-id")));
            state.Events.Clear();
            state.InvalidateSubOrchestrationInstanceIdIndex();
            Assert.IsNull(Validate(state, Child(1, "new-id")));
        }

        [TestMethod]
        public void InPlaceIdentityAndAliasedTagChangesAreRebuiltAfterInvalidation()
        {
            var tags = new Dictionary<string, string>();
            SubOrchestrationInstanceCreatedEvent created = Created(0, "old-id");
            created.Tags = tags;
            var state = new OrchestrationRuntimeState(new[] { created });
            Assert.IsNotNull(Validate(state, Child(1, "old-id")));
            created.InstanceId = "new-id";
            created.EventId = 2;
            state.InvalidateSubOrchestrationInstanceIdIndex();
            state.AddEvent(Completion(0, false));
            Assert.IsNull(Validate(state, Child(3, "old-id")));
            Assert.IsNotNull(Validate(state, Child(3, "new-id")));

            tags.Add(OrchestrationTags.FireAndForget, "");
            state.InvalidateSubOrchestrationInstanceIdIndex();
            Assert.IsNull(Validate(state, Child(3, "new-id")));
            tags.Remove(OrchestrationTags.FireAndForget);
            state.InvalidateSubOrchestrationInstanceIdIndex();
            Assert.IsNotNull(Validate(state, Child(3, "new-id")));
            state.AddEvent(Completion(2, true));
            Assert.IsNull(Validate(state, Child(3, "new-id")));
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void CompletionCorrelationReplacementIsRebuiltAfterInvalidation(bool failed)
        {
            HistoryEvent completion = Completion(0, failed);
            var state = new OrchestrationRuntimeState(new[]
            {
                Created(0, "first"), Created(1, "second"), completion,
            });
            Assert.IsNull(Validate(state, Child(2, "first")));
            Assert.IsNotNull(Validate(state, Child(2, "second")));
            state.Events[2] = Completion(1, failed);
            state.InvalidateSubOrchestrationInstanceIdIndex();
            Assert.IsNotNull(Validate(state, Child(2, "first")));
            Assert.IsNull(Validate(state, Child(2, "second")));
        }

        [DataTestMethod]
        [DataRow(TypeNameHandling.Objects)]
        [DataRow(TypeNameHandling.Auto)]
        [DataRow(TypeNameHandling.All)]
        public void DerivedIndexDoesNotChangeSerializedHistoryOrRuntimeState(TypeNameHandling typeNameHandling)
        {
            var state = new OrchestrationRuntimeState(new HistoryEvent[]
            {
                new ExecutionStartedEvent(-1, null)
                {
                    Name = "parent",
                    Version = "",
                    OrchestrationInstance = new OrchestrationInstance { InstanceId = "parent-id", ExecutionId = "execution-id" },
                },
                Created(0, "child-id"),
            });
            Assert.IsInstanceOfType(state.Events, typeof(List<HistoryEvent>));
            var converter = new JsonDataConverter(new JsonSerializerSettings { TypeNameHandling = typeNameHandling });
            string history = converter.Serialize(new OrchestrationSessionState(state.Events));
            string runtime = converter.Serialize(state);
            Assert.IsNotNull(Validate(state, Child(1, "child-id")));
            Assert.AreEqual(history, converter.Serialize(new OrchestrationSessionState(state.Events)));
            Assert.AreEqual(runtime, converter.Serialize(state));
            state.InvalidateSubOrchestrationInstanceIdIndex();
            Assert.AreEqual(runtime, converter.Serialize(state));
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void RepeatedCreatedTaskIdsReplaceRatherThanDuplicateMembership(bool initializeBeforeDuplicates)
        {
            var state = new OrchestrationRuntimeState(new[]
            {
                Created(0, "first"), Created(1, "first"),
            });
            if (initializeBeforeDuplicates)
            {
                Assert.IsNotNull(Validate(state, Child(2, "first")));
            }

            state.AddEvent(Created(0, "first"));
            state.AddEvent(Created(0, "first"));
            Assert.IsNotNull(Validate(state, Child(2, "first")));
            state.AddEvent(Completion(0, false));
            OrchestrationCompleteOrchestratorAction failure = Validate(state, Child(2, "first"));
            Assert.IsNotNull(failure);
            StringAssert.Contains(failure.Result, "task ID 1");
            state.AddEvent(Created(1, "second"));
            Assert.IsNull(Validate(state, Child(2, "first")));
            Assert.IsNotNull(Validate(state, Child(2, "second")));
            state.AddEvent(Completion(0, false));
            Assert.IsNotNull(Validate(state, Child(2, "second")));
            state.AddEvent(Completion(1, true));
            Assert.IsNull(Validate(state, Child(2, "second")));
        }

        [TestMethod]
        public void DrainedFanoutReleasesHistoricalPeakDictionaryCapacity()
        {
            var state = new OrchestrationRuntimeState();
            Assert.IsNull(Validate(state, Child(1000, "next")));
            for (int i = 0; i < 1000; i++)
            {
                state.AddEvent(Created(i, "child-" + i));
            }

            SubOrchestrationInstanceIdIndex index = state.GetSubOrchestrationInstanceIdIndex();
            for (int i = 0; i < 1000; i++)
            {
                state.AddEvent(Completion(i, false));
            }

            Assert.AreSame(index, state.GetSubOrchestrationInstanceIdIndex());
            Assert.IsNull(typeof(SubOrchestrationInstanceIdIndex)
                .GetField("pendingTasks", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(index));
            Assert.IsNull(typeof(SubOrchestrationInstanceIdIndex)
                .GetField("pendingInstances", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(index));
            Assert.IsNull(Validate(state, Child(1000, "child-0")));
        }

        [TestMethod]
        public void ColdCompletedSequentialHistoryDoesNotAllocatePendingNodeStorage()
        {
            var history = new List<HistoryEvent>();
            for (int i = 0; i < 1000; i++)
            {
                history.Add(Created(i, "child-" + i));
                history.Add(Completion(i, false));
            }

            var state = new OrchestrationRuntimeState(history);
            Assert.IsNull(Validate(state, Child(1000, "next")));
            SubOrchestrationInstanceIdIndex index = state.GetSubOrchestrationInstanceIdIndex();
            Assert.IsNull(typeof(SubOrchestrationInstanceIdIndex)
                .GetField("pendingTasks", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(index));
            Assert.IsNull(typeof(SubOrchestrationInstanceIdIndex)
                .GetField("pendingInstances", BindingFlags.NonPublic | BindingFlags.Instance).GetValue(index));
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public async Task RuntimeStreamRestoreRebuildsPendingChildren(bool compressed)
        {
            var state = new OrchestrationRuntimeState(new[] { Created(0, "child-id") });
            Assert.IsNotNull(Validate(state, Child(1, "child-id")));
            using var stream = await RuntimeStateStreamConverter.OrchestrationRuntimeStateToRawStream(
                state, state, JsonDataConverter.Default, compressed, new SessionSettings(), null, "parent-id");
            var restored = await RuntimeStateStreamConverter.RawStreamToRuntimeState(
                stream, "parent-id", null, JsonDataConverter.Default);
            Assert.IsNotNull(Validate(restored, Child(1, "child-id")));
            restored.AddEvent(Completion(0, true));
            Assert.IsNull(Validate(restored, Child(1, "child-id")));
            Assert.IsNotNull(Validate(state, Child(1, "child-id")));
        }

        sealed class SessionSettings : ISessionSettings
        {
            public int SessionMaxSizeInBytes { get; set; } = 1024 * 1024;

            public int SessionOverflowThresholdInBytes { get; set; } = 1024 * 1024;
        }

        sealed class CountingTags : Dictionary<string, string>, IDictionary<string, string>
        {
            public int Reads { get; private set; }

            bool IDictionary<string, string>.ContainsKey(string key)
            {
                this.Reads++;
                return base.ContainsKey(key);
            }
        }

        static OrchestrationCompleteOrchestratorAction Validate(IEnumerable<HistoryEvent> history, params OrchestratorAction[] decisions)
            => Validate(new OrchestrationRuntimeState(history.ToList()), decisions);

        static OrchestrationCompleteOrchestratorAction Validate(OrchestrationRuntimeState state, params OrchestratorAction[] decisions)
            => SubOrchestrationInstanceIdValidator.GetFailure("parent-id", state, decisions);

        static CreateSubOrchestrationAction Child(int taskId, string instanceId)
            => new CreateSubOrchestrationAction { Id = taskId, InstanceId = instanceId, Name = "child", Version = "" };

        static SubOrchestrationInstanceCreatedEvent Created(int taskId, string instanceId)
            => new SubOrchestrationInstanceCreatedEvent(taskId) { InstanceId = instanceId };

        static HistoryEvent Completion(int taskId, bool failed)
            => failed
                ? (HistoryEvent)new SubOrchestrationInstanceFailedEvent(-1, taskId, "failure", null)
                : new SubOrchestrationInstanceCompletedEvent(-1, taskId, "result");
    }
}
