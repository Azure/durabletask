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
    using System.Threading.Tasks;
    using DurableTask.Core.Command;
    using DurableTask.Core.History;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

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
            IEnumerable<HistoryEvent> history = Enumerable.Range(0, 1)
                .Select<int, HistoryEvent>(_ => throw new InvalidOperationException("History should not be scanned."));
            CreateSubOrchestrationAction detached = Child(1, "child-id");
            detached.Tags = new Dictionary<string, string> { { OrchestrationTags.FireAndForget, "" } };
            Assert.IsNull(Validate(history));
            Assert.IsNull(Validate(history, detached, new CreateTimerOrchestratorAction { Id = 2 }));
        }

        static OrchestrationCompleteOrchestratorAction Validate(IEnumerable<HistoryEvent> history, params OrchestratorAction[] decisions)
            => SubOrchestrationInstanceIdValidator.GetFailure("parent-id", history, decisions);

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
