// ---------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ---------------------------------------------------------------

namespace DurableTask.Core.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for the per-attempt retry tag injection added to
    /// <see cref="OrchestrationContext.ScheduleWithRetry{T}(string, string, RetryOptions, object[])"/>.
    /// </summary>
    [TestClass]
    public class ScheduleWithRetryTagsTests
    {
        private MockContext context;
        private OrchestrationInstance instance;

        [TestInitialize]
        public void Initialize()
        {
            this.instance = new OrchestrationInstance { InstanceId = "test", ExecutionId = Guid.NewGuid().ToString() };
            this.context = new MockContext(this.instance, TaskScheduler.Default);
        }

        // ---------------------------------------------------------------------
        // Core test #1: tags are written per attempt, 1-based and monotonically increasing.
        // ---------------------------------------------------------------------
        [TestMethod]
        public async Task ScheduleWithRetry_AllAttemptsFail_TagsAre1Based_AndMonotonic()
        {
            // Arrange
            const int maxAttempts = 3;
            RetryOptions retry = new RetryOptions(TimeSpan.FromMilliseconds(1), maxAttempts);

            // Act: schedule, then fail every attempt so the policy runs to exhaustion.
            Task<int> resultTask = this.context.ScheduleWithRetry<int>("Act", "1.0", retry, "p");
            await this.FailEveryScheduledTaskAsync(maxAttempts);

            // The orchestration call should ultimately throw.
            await Assert.ThrowsExceptionAsync<DurableTask.Core.Exceptions.TaskFailedException>(() => resultTask);

            // Assert: exactly maxAttempts scheduled tasks, each carrying the correct tags.
            Assert.AreEqual(maxAttempts, this.context.ScheduledTasks.Count);
            for (int i = 0; i < maxAttempts; i++)
            {
                ScheduledTaskInfo info = this.context.ScheduledTasks[i];
                Assert.IsNotNull(info.Options, $"Attempt {i + 1} should have options.");
                Assert.IsNotNull(info.Options.Tags, $"Attempt {i + 1} should have tags.");

                Assert.AreEqual((i + 1).ToString(CultureInfo.InvariantCulture), info.Options.Tags[RetryTags.Attempt]);
                Assert.AreEqual(maxAttempts.ToString(CultureInfo.InvariantCulture), info.Options.Tags[RetryTags.MaxAttempts]);
            }
        }

        // ---------------------------------------------------------------------
        // Core test #1 (variant): activity succeeds on attempt 2 — only 2 schedules recorded.
        // ---------------------------------------------------------------------
        [TestMethod]
        public async Task ScheduleWithRetry_SucceedsOnAttempt2_OnlyTwoScheduledTasks()
        {
            // Arrange
            RetryOptions retry = new RetryOptions(TimeSpan.FromMilliseconds(1), 5);

            // Act
            Task<int> resultTask = this.context.ScheduleWithRetry<int>("Act", "1.0", retry, "p");

            // Attempt 1 fails, attempt 2 succeeds.
            this.context.FailLastScheduledTask(new InvalidOperationException("transient"));
            this.context.CompleteLastScheduledTask(42);

            int result = await resultTask;

            // Assert
            Assert.AreEqual(42, result);
            Assert.AreEqual(2, this.context.ScheduledTasks.Count);

            Assert.AreEqual("1", this.context.ScheduledTasks[0].Options.Tags[RetryTags.Attempt]);
            Assert.AreEqual("2", this.context.ScheduledTasks[1].Options.Tags[RetryTags.Attempt]);
            Assert.AreEqual("5", this.context.ScheduledTasks[0].Options.Tags[RetryTags.MaxAttempts]);
            Assert.AreEqual("5", this.context.ScheduledTasks[1].Options.Tags[RetryTags.MaxAttempts]);
        }

        // ---------------------------------------------------------------------
        // Attempt values that cross into double digits are formatted as
        // plain multi-digit decimal strings ("10", "11", "12") — no zero-padding, separators, or
        // culture-specific digits.
        // ---------------------------------------------------------------------
        [TestMethod]
        public async Task ScheduleWithRetry_AttemptsCrossIntoDoubleDigits_TagsAreMultiDigitDecimal()
        {
            // Arrange: a policy ceiling high enough to push the attempt counter past 9.
            const int maxAttempts = 12;
            RetryOptions retry = new RetryOptions(TimeSpan.FromMilliseconds(1), maxAttempts);

            // Fail every attempt so all 12 schedules land.
            Task<int> resultTask = this.context.ScheduleWithRetry<int>("Act", "1.0", retry, "p");
            await this.FailEveryScheduledTaskAsync(maxAttempts);
            await Assert.ThrowsExceptionAsync<DurableTask.Core.Exceptions.TaskFailedException>(() => resultTask);

            // Every attempt tag equals its 1-based index rendered as an invariant decimal,
            // including the multi-digit values 10, 11, and 12.
            Assert.AreEqual(maxAttempts, this.context.ScheduledTasks.Count);
            for (int i = 0; i < maxAttempts; i++)
            {
                Assert.AreEqual(
                    (i + 1).ToString(CultureInfo.InvariantCulture),
                    this.context.ScheduledTasks[i].Options.Tags[RetryTags.Attempt]);
                Assert.AreEqual("12", this.context.ScheduledTasks[i].Options.Tags[RetryTags.MaxAttempts]);
            }

            // Explicit spot-checks across the single-/double-digit boundary.
            Assert.AreEqual("9", this.context.ScheduledTasks[8].Options.Tags[RetryTags.Attempt]);
            Assert.AreEqual("10", this.context.ScheduledTasks[9].Options.Tags[RetryTags.Attempt]);
            Assert.AreEqual("12", this.context.ScheduledTasks[11].Options.Tags[RetryTags.Attempt]);
        }

        // ---------------------------------------------------------------------
        // Each ScheduleWithRetry call owns an independent attempt counter, so a new call site
        // begins at attempt 1 instead of inheriting the previous call's count.
        // ---------------------------------------------------------------------
        [TestMethod]
        public async Task ScheduleWithRetry_MultipleCallSites_EachHasOwnCounter()
        {
            // Arrange
            RetryOptions retry = new RetryOptions(TimeSpan.FromMilliseconds(1), 3);

            // Act: kick off two independent retry sequences.
            Task<int> first = this.context.ScheduleWithRetry<int>("ActA", "1.0", retry);
            this.context.FailLastScheduledTask(new InvalidOperationException("a-fail"));
            this.context.CompleteLastScheduledTask(1);
            int firstResult = await first;

            Task<int> second = this.context.ScheduleWithRetry<int>("ActB", "1.0", retry);
            this.context.CompleteLastScheduledTask(2);
            int secondResult = await second;

            // Assert: first sequence has attempts 1,2. Second has attempt 1 only.
            Assert.AreEqual(1, firstResult);
            Assert.AreEqual(2, secondResult);
            Assert.AreEqual(3, this.context.ScheduledTasks.Count);

            Assert.AreEqual("ActA", this.context.ScheduledTasks[0].Name);
            Assert.AreEqual("1", this.context.ScheduledTasks[0].Options.Tags[RetryTags.Attempt]);

            Assert.AreEqual("ActA", this.context.ScheduledTasks[1].Name);
            Assert.AreEqual("2", this.context.ScheduledTasks[1].Options.Tags[RetryTags.Attempt]);

            // ActB starts a fresh counter at 1 — not 3.
            Assert.AreEqual("ActB", this.context.ScheduledTasks[2].Name);
            Assert.AreEqual("1", this.context.ScheduledTasks[2].Options.Tags[RetryTags.Attempt]);
        }

        // ---------------------------------------------------------------------
        // Deterministic replay test
        // ---------------------------------------------------------------------
        [TestMethod]
        public async Task ScheduleWithRetry_Replay_ReproducesIdenticalTags_AndContinuesCounter()
        {
            const int maxAttempts = 5;

            // Original execution: run up to the "crash" point (attempts 1 and 2 fail)
            RetryOptions originalRetry = new RetryOptions(TimeSpan.FromMilliseconds(1), maxAttempts);
            _ = this.context.ScheduleWithRetry<int>("Act", "1.0", originalRetry, "p");
            this.context.FailLastScheduledTask(new InvalidOperationException("attempt-1"));
            this.context.FailLastScheduledTask(new InvalidOperationException("attempt-2"));

            string originalAttempt1 = this.context.ScheduledTasks[0].Options.Tags[RetryTags.Attempt];
            string originalAttempt2 = this.context.ScheduledTasks[1].Options.Tags[RetryTags.Attempt];
            // The original schedule call is intentionally left pending — it models the instance crashing before attempt 3.

            // Replay: a fresh context re-walks the same history with IsReplaying = true
            var replayContext = new MockContext(this.instance, TaskScheduler.Default) { IsReplaying = true };
            RetryOptions replayRetry = new RetryOptions(TimeSpan.FromMilliseconds(1), maxAttempts);
            Task<int> replayTask = replayContext.ScheduleWithRetry<int>("Act", "1.0", replayRetry, "p");

            // Re-walk the recorded failures for attempts 1 and 2
            replayContext.FailLastScheduledTask(new InvalidOperationException("attempt-1"));
            replayContext.FailLastScheduledTask(new InvalidOperationException("attempt-2"));

            // Continue past the crash point: attempt 3 succeeds.
            replayContext.CompleteLastScheduledTask(99);
            int replayResult = await replayTask;

            // Assert: the replayed attempts are byte-for-byte identical to the original run,
            // even though the replay context advertised IsReplaying = true.
            Assert.AreEqual(originalAttempt1, replayContext.ScheduledTasks[0].Options.Tags[RetryTags.Attempt]);
            Assert.AreEqual(originalAttempt2, replayContext.ScheduledTasks[1].Options.Tags[RetryTags.Attempt]);
            Assert.AreEqual("1", replayContext.ScheduledTasks[0].Options.Tags[RetryTags.Attempt]);
            Assert.AreEqual("2", replayContext.ScheduledTasks[1].Options.Tags[RetryTags.Attempt]);

            // Assert: replay proceeded to schedule attempt 3 with attempt=3 and the unchanged ceiling.
            Assert.AreEqual(3, replayContext.ScheduledTasks.Count);
            Assert.AreEqual("3", replayContext.ScheduledTasks[2].Options.Tags[RetryTags.Attempt]);
            Assert.AreEqual("5", replayContext.ScheduledTasks[2].Options.Tags[RetryTags.MaxAttempts]);
            Assert.AreEqual(99, replayResult);
        }

        // ---------------------------------------------------------------------
        // Core test #3: non-retry ScheduleTask on the same context does NOT emit retry tags.
        // ---------------------------------------------------------------------
        [TestMethod]
        public async Task ScheduleTask_WithoutRetryPolicy_DoesNotEmitRetryTags()
        {
            // Act
            Task<int> resultTask = this.context.ScheduleTask<int>("Act", "1.0", "p");
            this.context.CompleteLastScheduledTask(0);
            await resultTask;

            // Assert: the non-retry ScheduleTask override in the mock records `Options=null`.
            Assert.AreEqual(1, this.context.ScheduledTasks.Count);
            Assert.IsNull(this.context.ScheduledTasks[0].Options);
        }

        // ---------------------------------------------------------------------
        // Core test #3 (variant): non-retry interleaved with retry — only retry calls get tags.
        // ---------------------------------------------------------------------
        [TestMethod]
        public async Task ScheduleTask_InterleavedNonRetryAndRetry_OnlyRetryCallsHaveTags()
        {
            // Arrange
            RetryOptions retry = new RetryOptions(TimeSpan.FromMilliseconds(1), 2);

            // Act: non-retry then retry then non-retry.
            Task<int> first = this.context.ScheduleTask<int>("Plain", "1.0");
            this.context.CompleteLastScheduledTask(1);
            await first;

            Task<int> second = this.context.ScheduleWithRetry<int>("Retried", "1.0", retry);
            this.context.CompleteLastScheduledTask(2);
            await second;

            Task<int> third = this.context.ScheduleTask<int>("Plain2", "1.0");
            this.context.CompleteLastScheduledTask(3);
            await third;

            // Assert
            Assert.AreEqual(3, this.context.ScheduledTasks.Count);
            Assert.IsNull(this.context.ScheduledTasks[0].Options, "Plain (1st) should have no options.");
            Assert.IsNotNull(this.context.ScheduledTasks[1].Options.Tags, "Retried call should have retry tags.");
            Assert.AreEqual("1", this.context.ScheduledTasks[1].Options.Tags[RetryTags.Attempt]);
            Assert.IsNull(this.context.ScheduledTasks[2].Options, "Plain2 (3rd) should have no options.");
        }

        // ---------------------------------------------------------------------
        // Core test #5 (extended): Handle()-rejected exception short-circuits before policy ceiling.
        // The injection at attempt N still happens (it ran before the throw),
        // but no attempt N+1 should ever be scheduled.
        // ---------------------------------------------------------------------
        [TestMethod]
        public async Task ScheduleWithRetry_HandleRejectsException_NoFurtherAttemptsScheduled()
        {
            // Arrange
            RetryOptions retry = new RetryOptions(TimeSpan.FromMilliseconds(1), 5);
            retry.Handle = ex => ex.Message != "permanent";

            // Act
            Task<int> resultTask = this.context.ScheduleWithRetry<int>("Act", "1.0", retry);

            // Attempt 1 fails with a transient error (Handle returns true → retry).
            this.context.FailLastScheduledTask(new InvalidOperationException("transient"));

            // Attempt 2 fails with a permanent error (Handle returns false → loop stops).
            this.context.FailLastScheduledTask(new InvalidOperationException("permanent"));

            // The orchestration call should ultimately throw the permanent exception.
            await Assert.ThrowsExceptionAsync<DurableTask.Core.Exceptions.TaskFailedException>(() => resultTask);

            // Assert: ONLY two attempts scheduled, despite policy ceiling of 5.
            // This is the key assertion the design called out — a buggy Handle()-ignoring
            // implementation would schedule attempts 3, 4, 5 too.
            Assert.AreEqual(2, this.context.ScheduledTasks.Count);
            Assert.AreEqual("1", this.context.ScheduledTasks[0].Options.Tags[RetryTags.Attempt]);
            Assert.AreEqual("2", this.context.ScheduledTasks[1].Options.Tags[RetryTags.Attempt]);
            Assert.AreEqual("5", this.context.ScheduledTasks[1].Options.Tags[RetryTags.MaxAttempts]);
        }

        // ---------------------------------------------------------------------
        // Core test #4: maxAttempts=1 policy still emits tags (attempt=1, maxAttempts=1).
        // ---------------------------------------------------------------------
        [TestMethod]
        public async Task ScheduleWithRetry_MaxAttempts1_EmitsTagsAndConsumesOnlyAttempt()
        {
            // Arrange
            RetryOptions retry = new RetryOptions(TimeSpan.FromMilliseconds(1), 1);

            // Act
            Task<int> resultTask = this.context.ScheduleWithRetry<int>("Act", "1.0", retry);
            this.context.FailLastScheduledTask(new InvalidOperationException("boom"));
            await Assert.ThrowsExceptionAsync<DurableTask.Core.Exceptions.TaskFailedException>(() => resultTask);

            // Assert: exactly 1 schedule, tagged attempt=1, maxAttempts=1.
            Assert.AreEqual(1, this.context.ScheduledTasks.Count);
            Assert.AreEqual("1", this.context.ScheduledTasks[0].Options.Tags[RetryTags.Attempt]);
            Assert.AreEqual("1", this.context.ScheduledTasks[0].Options.Tags[RetryTags.MaxAttempts]);
        }

        // ---------------------------------------------------------------------
        // Core test #8: MaxNumberOfAttempts == 0 — closure is never invoked.
        // Returns default(T) silently (verified from RetryInterceptor.Invoke source).
        // ---------------------------------------------------------------------
        [TestMethod]
        public async Task ScheduleWithRetry_MaxAttempts0_ClosureNeverInvoked_ReturnsDefault()
        {
            // Arrange
            RetryOptions retry = new RetryOptions(TimeSpan.FromMilliseconds(1), 0);

            // Act
            int result = await this.context.ScheduleWithRetry<int>("Act", "1.0", retry);

            // Assert
            Assert.AreEqual(0, result, "default(int) should be returned when MaxNumberOfAttempts is 0.");
            Assert.AreEqual(0, this.context.ScheduledTasks.Count, "No schedule should have happened.");
        }

        // ---------------------------------------------------------------------
        // Locale resilience: tags MUST be ASCII decimal regardless of current culture.
        // From the History tag contract: CultureInfo.InvariantCulture is required.
        // ---------------------------------------------------------------------
        [TestMethod]
        public async Task ScheduleWithRetry_NonInvariantCulture_TagsStillAsciiDecimal()
        {
            // Arrange: switch thread culture to one that uses non-ASCII digits.
            CultureInfo original = CultureInfo.CurrentCulture;
            try
            {
                Thread.CurrentThread.CurrentCulture = new CultureInfo("ar-SA"); // Arabic-Saudi Arabia uses Arabic-Indic digits in some formats.
                RetryOptions retry = new RetryOptions(TimeSpan.FromMilliseconds(1), 3);

                // Act
                Task<int> resultTask = this.context.ScheduleWithRetry<int>("Act", "1.0", retry);
                await this.FailEveryScheduledTaskAsync(3);
                await Assert.ThrowsExceptionAsync<DurableTask.Core.Exceptions.TaskFailedException>(() => resultTask);

                // Assert: every tag value parses as a plain ASCII decimal integer.
                foreach (ScheduledTaskInfo info in this.context.ScheduledTasks)
                {
                    string attempt = info.Options.Tags[RetryTags.Attempt];
                    string maxAttempts = info.Options.Tags[RetryTags.MaxAttempts];
                    Assert.IsTrue(int.TryParse(attempt, System.Globalization.NumberStyles.None, CultureInfo.InvariantCulture, out _),
                        $"attempt tag '{attempt}' must be strict-decimal ASCII.");
                    Assert.IsTrue(int.TryParse(maxAttempts, System.Globalization.NumberStyles.None, CultureInfo.InvariantCulture, out _),
                        $"maxAttempts tag '{maxAttempts}' must be strict-decimal ASCII.");
                }
            }
            finally
            {
                Thread.CurrentThread.CurrentCulture = original;
            }
        }

        // -----------------------------------------------------------------
        // Helpers
        // -----------------------------------------------------------------

        /// <summary>
        /// Fails the most recently scheduled task and then waits for the retry interceptor's
        /// timer to advance, repeating until <paramref name="attempts"/> tasks have been failed.
        /// </summary>
        private async Task FailEveryScheduledTaskAsync(int attempts)
        {
            for (int i = 0; i < attempts; i++)
            {
                // Wait for the next schedule to land in the mock's list.
                int expectedCount = i + 1;
                int spins = 0;
                while (this.context.ScheduledTasks.Count < expectedCount && spins++ < 1000)
                {
                    await Task.Yield();
                }
                Assert.AreEqual(expectedCount, this.context.ScheduledTasks.Count,
                    $"Schedule for attempt {expectedCount} should have arrived.");
                this.context.FailLastScheduledTask(new InvalidOperationException($"fail-{expectedCount}"));
            }
        }

        // -----------------------------------------------------------------
        // Mock context — small, self-contained. Records every schedule along with the
        // ScheduleTaskOptions that the call site passed (so tests can inspect Tags).
        // -----------------------------------------------------------------
        private sealed class MockContext : TaskOrchestrationContext
        {
            public List<ScheduledTaskInfo> ScheduledTasks { get; } = new List<ScheduledTaskInfo>();

            public MockContext(OrchestrationInstance instance, TaskScheduler scheduler)
                : base(instance, scheduler)
            {
                this.CurrentUtcDateTime = DateTime.UtcNow;
            }

            public override Task<T> CreateTimer<T>(DateTime fireAt, T state, CancellationToken cancelToken)
            {
                // Skip the delay; advance the simulated clock and return immediately.
                this.CurrentUtcDateTime = fireAt;
                return Task.FromResult(state);
            }

            public override async Task<TResult> ScheduleTask<TResult>(string name, string version, params object[] parameters)
            {
                this.ScheduledTasks.Add(new ScheduledTaskInfo { Name = name, Version = version, Parameters = parameters, Options = null });
                return await base.ScheduleTask<TResult>(name, version, parameters);
            }

            public override async Task<TResult> ScheduleTask<TResult>(string name, string version, ScheduleTaskOptions options, params object[] parameters)
            {
                this.ScheduledTasks.Add(new ScheduledTaskInfo { Name = name, Version = version, Parameters = parameters, Options = options });
                return await base.ScheduleTask<TResult>(name, version, options, parameters);
            }

            public void CompleteLastScheduledTask<T>(T result)
            {
                int taskIndex = this.ScheduledTasks.Count - 1;
                string serialized = this.MessageDataConverter.SerializeInternal(result);
                this.HandleTaskCompletedEvent(new History.TaskCompletedEvent(0, taskIndex, serialized));
            }

            public void FailLastScheduledTask(Exception exception)
            {
                int taskIndex = this.ScheduledTasks.Count - 1;
                string details = this.ErrorDataConverter.SerializeInternal(exception);
                this.HandleTaskFailedEvent(new History.TaskFailedEvent(0, taskIndex, exception.Message, details));
            }
        }

        private sealed class ScheduledTaskInfo
        {
            public string Name { get; set; }
            public string Version { get; set; }
            public object[] Parameters { get; set; }
            public ScheduleTaskOptions Options { get; set; }
        }
    }
}
